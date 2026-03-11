from dataclasses import dataclass
from typing import Optional

from pandera.polars import DataFrameSchema


@dataclass
class TableDefinition:
    name: str
    schema: DataFrameSchema
    title: Optional[str] = None
    db_schema: Optional[str] = None


@dataclass
class DatabaseDefinition:
    name: str
    tables: list[TableDefinition]
