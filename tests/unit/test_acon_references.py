"""Every reference in every ACON must point at code that exists.

A callable or contract that has been moved or renamed is invisible until the
pipeline runs, and Spark-skipped tests hide it locally — that is exactly how a
stale reference reaches a scheduled job.

References are resolved by parsing the target module rather than importing it,
so these checks run anywhere, including environments without pyspark. Assertions
that genuinely need the class (a contract's table name and primary key) import
it and skip when Spark is absent.
"""
import ast
from pathlib import Path

import pytest

from lakehouse_platform.core.acon import Acon
from lakehouse_platform.io.readers import read_input

ROOT = Path(__file__).resolve().parents[2]
ACONS = sorted(ROOT.glob("products/*/pipelines/*.yaml"))
IDS = [str(path.relative_to(ROOT)) for path in ACONS]

# Readers and writers read_input/write_output implement. Listed explicitly so an
# accidental removal shows up here instead of at runtime.
SUPPORTED_READERS = {
    "unity_catalog_table", "json", "json_records", "csv_records", "text", "product_callable",
}
SUPPORTED_WRITERS = {"delta_table", "delta_merge"}


def _module_path(module: str) -> Path:
    """Resolve a dotted module to a file, honouring the src layout.

    Products live at the repository root; the platform lives under ``src/``. An
    ACON may reference either, so both roots are tried.
    """
    relative = module.replace(".", "/") + ".py"
    candidates = [ROOT / relative, ROOT / "src" / relative]
    return next((path for path in candidates if path.is_file()), candidates[0])


def _top_level_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def assert_reference_exists(reference: str, source: Path) -> None:
    module, _, attribute = reference.partition(":")
    assert attribute, f"{source}: '{reference}' must use 'module:attribute'"
    path = _module_path(module)
    assert path.is_file(), f"{source}: '{reference}' points at missing module {path}"
    assert attribute in _top_level_names(path), (
        f"{source}: '{attribute}' does not exist in {module}"
    )


def _function_signature(reference: str, source: Path) -> ast.arguments:
    module, _, attribute = reference.partition(":")
    tree = ast.parse(_module_path(module).read_text(encoding="utf-8"))
    node = next(
        (
            item for item in tree.body
            if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef)
            and item.name == attribute
        ),
        None,
    )
    assert node is not None, f"{source}: '{attribute}' is not a function in {module}"
    return node.args


def _import_or_skip(reference: str):
    from lakehouse_platform.core.imports import import_callable

    try:
        return import_callable(reference)
    except ModuleNotFoundError as error:  # pragma: no cover - environment dependent
        if error.name == "pyspark":
            pytest.skip("pyspark not installed")
        raise


def test_products_declare_acons():
    assert ACONS, "no product ACON files found"


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_acon_is_structurally_valid(path):
    Acon.from_yaml(path)  # raises AconError on bad ids, references or missing fields


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_every_reference_points_at_real_code(path):
    acon = Acon.from_yaml(path)
    for spec in acon.transformations:
        assert_reference_exists(spec.callable, path)
    for spec in acon.inputs:
        if spec.kind == "product_callable":
            assert_reference_exists(spec.options["callable"], path)
    for spec in acon.outputs:
        if spec.contract:
            assert_reference_exists(spec.contract, path)


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_every_transformation_accepts_the_options_the_engine_passes(path):
    """The engine always calls ``transform(frame, options)``.

    A transformation written as ``transform(df)`` imports cleanly, passes every
    reference check, and then raises TypeError the first time it runs on a
    cluster. Checking the arity here is the difference between a failed test and
    a failed 03:00 job.
    """
    for spec in Acon.from_yaml(path).transformations:
        arguments = _function_signature(spec.callable, path)
        positional = [argument.arg for argument in arguments.posonlyargs + arguments.args]
        frames = len(spec.input_ids) if spec.input_ids else 1
        required = frames + 1  # the frames, then options
        assert len(positional) >= required or arguments.vararg, (
            f"{path}: {spec.callable} takes {positional}, but the engine calls it "
            f"with {frames} frame(s) plus options"
        )


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_readers_and_writers_are_implemented(path):
    acon = Acon.from_yaml(path)
    for spec in acon.inputs:
        assert spec.kind in SUPPORTED_READERS, f"{path}: unknown reader {spec.kind}"
    for spec in acon.outputs:
        assert spec.kind in SUPPORTED_WRITERS, f"{path}: unknown writer {spec.kind}"


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_output_contract_describes_the_table_being_written(path):
    for spec in Acon.from_yaml(path).outputs:
        if not spec.contract:
            continue
        contract = _import_or_skip(spec.contract)
        target = spec.options["table"].replace("${catalog}.", "")
        assert target == contract.object_location(), (
            f"{path}: writes {target} but the contract describes "
            f"{contract.object_location()}"
        )


@pytest.mark.parametrize("path", ACONS, ids=IDS)
def test_merge_keys_match_the_contract_primary_key(path):
    """Merging on anything but the contract PK would silently allow duplicates."""
    for spec in Acon.from_yaml(path).outputs:
        if spec.kind != "delta_merge" or not spec.contract:
            continue
        primary = _import_or_skip(spec.contract).primary_keys()
        if primary:
            assert list(spec.options.get("keys") or []) == primary, (
                f"{path}: merges on {spec.options.get('keys')} but the PK is {primary}"
            )


def test_read_input_rejects_an_unknown_reader():
    with pytest.raises(ValueError, match="unsupported reader"):
        read_input(None, "carrier_pigeon", {})
