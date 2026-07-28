"""DepMap RNA expression in long format.

The source is a wide matrix; storing it long (one row per cell line and gene)
means Delta can prune by gene, the schema does not change when DepMap adds
genes, and downstream feature selection is a filter rather than a column list.
"""
from dataclasses import dataclass
from datetime import datetime

from lakehouse_platform.schemas.base import BaseSchema
from lakehouse_platform.schemas.types import Double, String


@dataclass
class TableDefinition(BaseSchema):
    model_id: String
    gene_symbol: String
    expression_log1p_tpm: Double | None
    depmap_release: String
    ingested_at: datetime

    class Meta:
        object_name = "cell_expression"
        object_location = "silver.cell_expression"
        object_description = (
            "RNA expression per cell line and gene, log1p(TPM), unpivoted from "
            "the wide DepMap export. Roughly 1,800 cell lines x 19,000 genes."
        )
        column_constraints = {
            "model_id": {"PK": True},
            "gene_symbol": {"PK": True},
        }
        custom_table_properties = {"delta.enableChangeDataFeed": "true"}
        column_comments = {
            "model_id": "DepMap ModelID; joins to silver.cell_line.",
            "gene_symbol": "HGNC gene symbol as exported by DepMap.",
            "expression_log1p_tpm": "log1p-transformed TPM.",
        }


TABLE = TableDefinition.Meta.object_location
