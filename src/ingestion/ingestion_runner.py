"""Entry point for running configuration-driven ingestion.

Reads a source definition from ``config/sources`` and drives the generic
ingestion components (client, pagination, authentication) to land raw records
in the Bronze layer, recording batch metadata in the control tables.

This is a placeholder; the ingestion logic is implemented in Phase 2 of the
plan described in the README.
"""


def main() -> None:
    raise NotImplementedError("Ingestion runner is not yet implemented.")


if __name__ == "__main__":
    main()
