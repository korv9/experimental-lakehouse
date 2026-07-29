"""One command that checks the whole repository — used by CI and locally.

    python ci_check.py

Runs lint and the full test suite, prints one summary, and exits non-zero if
anything failed. The point is that the command CI runs is the same command you
run before pushing, so "green locally" and "green in CI" cannot drift apart.

The test suite itself stays split across ``tests/`` — a single giant test file
would make failures harder to locate, not easier. What is unified here is the
*entry point*, not the tests.

Spark is optional. Tests that need it skip themselves when pyspark is absent,
which keeps the fast path fast; ``--require-spark`` turns those skips into a
failure for the job that does install Spark.
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass

def _ruff_command() -> list[str]:
    """ruff ships as a standalone binary; only some versions support -m ruff."""
    executable = shutil.which("ruff")
    if executable:
        return [executable, "check", "."]
    return [sys.executable, "-m", "ruff", "check", "."]


def _checks() -> list[tuple[str, list[str]]]:
    return [
        ("lint", _ruff_command()),
        ("tests", [sys.executable, "-m", "pytest", "-q"]),
    ]


@dataclass
class Result:
    name: str
    ok: bool
    seconds: float
    summary: str


def _summarise(name: str, output: str) -> str:
    """Pull the one interesting line out of a tool's output."""
    if name == "tests":
        match = re.search(r"(\d+ passed.*?)(?:\s+in\s|\Z)", output)
        return match.group(1).strip() if match else "no test summary found"
    if "All checks passed" in output:
        return "no lint findings"
    match = re.search(r"Found (\d+) error", output)
    return f"{match.group(1)} lint findings" if match else "lint failed"


def _run(name: str, command: list[str]) -> Result:
    print(f"--- {name}: {' '.join(command)}")
    started = time.perf_counter()
    completed = subprocess.run(command, capture_output=True, text=True)
    output = completed.stdout + completed.stderr
    print(output.rstrip() or "(no output)")
    return Result(
        name=name,
        ok=completed.returncode == 0,
        seconds=time.perf_counter() - started,
        summary=_summarise(name, output),
    )


def _skipped_count(summary: str) -> int:
    match = re.search(r"(\d+) skipped", summary)
    return int(match.group(1)) if match else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--require-spark",
        action="store_true",
        help="fail if any test was skipped (for the CI job that installs pyspark)",
    )
    arguments = parser.parse_args()

    print("=" * 78)
    print("CI CHECK")
    print("=" * 78)

    results = [_run(name, command) for name, command in _checks()]

    print("=" * 78)
    for result in results:
        status = "PASS" if result.ok else "FAIL"
        print(f"[{status}] {result.name:<6} {result.summary}  ({result.seconds:.1f}s)")

    failed = [result for result in results if not result.ok]

    tests = next((result for result in results if result.name == "tests"), None)
    if arguments.require_spark and tests is not None:
        skipped = _skipped_count(tests.summary)
        spark_result = Result(
            name="spark",
            ok=skipped == 0,
            seconds=0.0,
            summary=("no tests skipped" if not skipped
                     else f"{skipped} skipped, but --require-spark was set"),
        )
        status = "PASS" if spark_result.ok else "FAIL"
        print(f"[{status}] {spark_result.name:<6} {spark_result.summary}")
        if not spark_result.ok:
            failed.append(spark_result)

    print("=" * 78)
    if failed:
        print("RESULT: FAIL —", ", ".join(sorted({result.name for result in failed})))
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
