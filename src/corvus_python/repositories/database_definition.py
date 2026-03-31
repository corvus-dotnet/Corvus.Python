from dataclasses import dataclass
from typing import Optional

from pandera.polars import DataFrameSchema


@dataclass
class TableDefinition:
    """Defines a single table within a database, including its Pandera schema and optional metadata.

    Attributes:
        name: The name of the table.
        schema: The Pandera DataFrameSchema used to validate data written to the table.
        title: An optional human-readable display name for the table.
        db_schema: An optional SQL schema name (e.g. "dbo") used when creating SQL views
            over this table in Synapse or Fabric. Has no effect on Delta table storage paths.
    """

    name: str
    schema: DataFrameSchema
    title: Optional[str] = None
    db_schema: Optional[str] = None


@dataclass
class DatabaseDefinition:
    """Defines a logical database consisting of a collection of tables.

    Attributes:
        name: The name of the database. Used as a path segment when resolving table storage paths.
        tables: The list of tables that belong to this database.
    """

    name: str
    tables: list[TableDefinition]
