import polars as pl
from deltalake import DeltaTable, write_deltalake
from opentelemetry import trace

from ..storage import (
    DataLakeLayer,
    StorageConfiguration,
)
from ..tracing import all_methods_start_new_current_span_with_method_name
from ..repositories import DatabaseDefinition, TableDefinition
from ..schema import pandera_polars_to_deltalake_schema

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class DeltaTableRepository:
    """
    A repository for managing Delta Lake tables.

    This class provides methods for reading, writing, and managing Delta tables
    within a specified data lake layer. It handles schema validation using Pandera
    and integrates with OpenTelemetry for tracing.
    """

    def __init__(
        self,
        storage_configuration: StorageConfiguration,
        data_lake_layer: DataLakeLayer,
        base_path: str,
        database_definition: DatabaseDefinition,
    ):
        """
        Initializes the DeltaTableRepository.

        Args:
            storage_configuration: Configuration for accessing storage.
            data_lake_layer: The data lake layer (e.g., Bronze, Silver, Gold).
            base_path: The base path within the data lake layer.
            database_definition: The definition of the database and its tables.
        """
        self.storage_configuration = storage_configuration
        self.data_lake_layer = data_lake_layer
        self.base_path = base_path
        self.database_definition = database_definition
        self.initialised = False
        self.storage_options = self.storage_configuration.storage_options
        self.pandera_schemas = {table.name: table.schema for table in self.database_definition.tables}

    def read_data(self, table_name: str) -> pl.DataFrame | None:
        """
        Reads data from a Delta table into a Polars DataFrame.

        Args:
            table_name: The name of the table to read.

        Returns:
            A Polars DataFrame containing the table data, or None if the table is empty.
        """
        path = self._get_table_path(table_name)

        # TODO: Potential perf improvement: Use `scan_delta` and return LazyFrame instead?
        df = pl.read_delta(path, storage_options=self.storage_options)

        return df

    def overwrite_table(
            self,
            table_name: str,
            data: pl.DataFrame | pl.LazyFrame,
            overwrite_schema: bool = False
    ):
        span = trace.get_current_span()

        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        span.set_attributes(
            {
                "row_count": data.height,
                "database_name": self.database_definition.name,
                "table_name": table_name,
            }
        )

        self.ensure_initialised()

        path = self._get_table_path(table_name)

        schema = self.pandera_schemas[table_name]

        schema.validate(data, lazy=False)

        data.write_delta(
            path,
            mode="overwrite",
            overwrite_schema=overwrite_schema,
            storage_options=self.storage_options,
        )

    def overwrite_table_with_condition(
            self,
            table_name: str,
            data: pl.DataFrame | pl.LazyFrame,
            predicate: str,
            overwrite_schema: bool = False
    ):
        span = trace.get_current_span()

        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        span.set_attributes(
            {
                "row_count": data.height,
                "database_name": self.database_definition.name,
                "table_name": table_name,
                "predicate": predicate,
            }
        )

        self.ensure_initialised()

        schema = self.pandera_schemas[table_name]

        schema.validate(data, lazy=False)

        path = self._get_table_path(table_name)

        write_deltalake(
            path,
            data.to_arrow(),  # type: ignore
            mode="overwrite",
            predicate=predicate,
            schema_mode=None,
            overwrite_schema=overwrite_schema,
            storage_options=self.storage_options,
        )

    def append_to_table(self, table_name: str, data: pl.DataFrame | pl.LazyFrame):
        span = trace.get_current_span()

        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        span.set_attributes(
            {
                "row_count": data.height,
                "database_name": self.database_definition.name,
                "table_name": table_name,
            }
        )

        self.ensure_initialised()

        self.pandera_schemas[table_name].validate(data, lazy=False)

        path = self._get_table_path(table_name)

        data.write_delta(
            path,
            mode="append",
            storage_options=self.storage_options,
        )

    def ensure_all_rows_match(self, data: pl.DataFrame, column_name: str, value: str):
        span = trace.get_current_span()
        span.set_attributes({"column_name": column_name, "value": value})

        if data.filter(pl.col(column_name) != value).height > 0:
            raise ValueError(f"Column '{column_name}' must have value '{value}' for all rows")

    def ensure_initialised(self):
        if not self.initialised:
            self.initialise_database()
            self.initialised = True

    def initialise_database(self):
        database_location = self.storage_configuration.get_full_path(
            self.data_lake_layer,
            f"{self.base_path}/{self.database_definition.name}",
        )

        span = trace.get_current_span()

        if (self.database_definition.tables is None) or (len(self.database_definition.tables) == 0):
            span.set_attribute("initialisation_required", False)
            return

        span.set_attributes(
            {
                "initialisation_required": True,
                "database_location": database_location,
                "database_name": self.database_definition.name,
            }
        )

        for table in self.database_definition.tables:
            self.initialise_table(table)

    def initialise_table(self, table: TableDefinition):
        span = trace.get_current_span()
        span.set_attributes({"database_name": self.database_definition.name, "table_name": table.name})

        table_path = self._get_table_path(table.name)

        try:
            DeltaTable(table_path, storage_options=self.storage_options)
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            schema = table.schema
            _: DeltaTable = DeltaTable.create(
                table_path,
                schema=pandera_polars_to_deltalake_schema(schema),
                storage_options=self.storage_options,
            )

    def _get_table_path(self, table_name: str) -> str:
        return self.storage_configuration.get_full_path(
            self.data_lake_layer,
            f"{self.base_path}/{self.database_definition.name}/{table_name}",
        )
