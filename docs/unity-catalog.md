# Unity Catalog conventions

The platform uses a catalog per environment and Unity Catalog's three-level
namespace for every governed object:

```text
<catalog>.<schema>.<table-or-volume>
```

The development catalog is `dev_lakehouse`. Production should use a separate
catalog rather than environment prefixes inside table names.

## Tables and volumes

Use managed Delta tables for tabular Bronze, Silver, Gold and operational
metadata. Use volumes for non-tabular files:

| Object | Purpose |
|---|---|
| `landing.source_files` | Downloaded books, HTML, XML and source snapshots |
| `platform.checkpoints` | Streaming, cursor and file-processing checkpoints |
| `platform.pipeline_runs` | Auditable job executions |
| `platform.ingestion_checkpoints` | Cursors, page numbers and watermarks |
| `platform.download_manifest` | URL, content hash, file path and retrieval state |

Volume paths always include catalog, schema and volume:

```text
/Volumes/<catalog>/landing/source_files/<product>/<source>/<file>
/Volumes/<catalog>/platform/checkpoints/<pipeline>/<state>
```

Volumes govern file access but are not SQL tables. Metadata required for
querying, lineage and idempotency belongs in Delta control tables.

Databricks recommends Unity Catalog volumes for non-tabular data and governed
locations for checkpoints. DBFS root and DBFS mounts are not used:

- [Unity Catalog volumes](https://docs.databricks.com/aws/en/volumes/)
- [Files in volumes](https://docs.databricks.com/aws/en/volumes/volume-files)
- [Auto Loader with Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog)
- [DBFS and Unity Catalog guidance](https://docs.databricks.com/aws/en/dbfs/unity-catalog)

## Checkpoint rules

- A page cursor is advanced only after its Bronze records are committed.
- Replayed source versions have deterministic `ingestion_id` values and are
  inserted with `MERGE ... WHEN NOT MATCHED`.
- File downloads use a same-directory `.part` file. The final filename is
  published only after length and SHA-256 validation.
- Structured Streaming and Auto Loader checkpoints belong in the governed
  checkpoint volume and must not be nested inside a table directory.
- Checkpoint locations must not have an object-storage lifecycle rule that can
  silently delete live state.

## Minimum job privileges

Replace `<job-principal>` and grant at the narrowest practical scope:

```sql
GRANT USE CATALOG ON CATALOG dev_lakehouse TO `<job-principal>`;
GRANT USE SCHEMA ON SCHEMA dev_lakehouse.landing TO `<job-principal>`;
GRANT USE SCHEMA ON SCHEMA dev_lakehouse.platform TO `<job-principal>`;
GRANT USE SCHEMA ON SCHEMA dev_lakehouse.bronze TO `<job-principal>`;

GRANT READ VOLUME, WRITE VOLUME
ON VOLUME dev_lakehouse.landing.source_files TO `<job-principal>`;

GRANT READ VOLUME, WRITE VOLUME
ON VOLUME dev_lakehouse.platform.checkpoints TO `<job-principal>`;

GRANT SELECT, MODIFY
ON TABLE dev_lakehouse.platform.pipeline_runs TO `<job-principal>`;

GRANT SELECT, MODIFY
ON TABLE dev_lakehouse.platform.ingestion_checkpoints TO `<job-principal>`;

GRANT SELECT, MODIFY
ON TABLE dev_lakehouse.platform.download_manifest TO `<job-principal>`;

GRANT CREATE TABLE
ON SCHEMA dev_lakehouse.bronze TO `<job-principal>`;
```

The principal that runs the one-time setup additionally needs permission to
create schemas and volumes. Runtime jobs should not receive catalog ownership
or broad administrative privileges.

## Setup

Run `notebooks/setup/00_create_platform.py` once per environment. The notebook
creates the configured catalog, layer schemas, managed landing/checkpoint
volumes and Delta control tables. In organizations where catalog creation is
admin-managed, provision the catalog separately and call
`create_unity_catalog_objects(..., create_catalog=False)`.
