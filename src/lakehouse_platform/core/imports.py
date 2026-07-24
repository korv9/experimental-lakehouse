"""Safe import helper for ACON callable references."""
from collections.abc import Callable
from importlib import import_module


def import_callable(reference: str) -> Callable:
    try:
        module_name, attribute = reference.split(":", 1)
    except ValueError as exc:
        raise ValueError(f"callable must use 'module:function': {reference}") from exc
    candidate = getattr(import_module(module_name), attribute)
    if not callable(candidate):
        raise TypeError(f"{reference} is not callable")
    return candidate
