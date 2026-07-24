"""Configuration-driven Spark lakehouse platform."""

__all__ = ["run_pipeline"]


def run_pipeline(*args, **kwargs):
    """Load the Spark-dependent engine only when a pipeline is executed."""
    from lakehouse_platform.engine import run_pipeline as execute

    return execute(*args, **kwargs)
