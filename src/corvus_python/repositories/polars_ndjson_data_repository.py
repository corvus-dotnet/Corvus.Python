import polars as pl
import logging
from opentelemetry import trace

from ..storage import StorageConfiguration, DataLakeLayer
from ..monitoring import all_methods_start_new_current_span_with_method_name

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class PolarsNdJsonDataRepository:
    def __init__(
        self,
        file_system_configuration: StorageConfiguration,
        data_lake_layer: DataLakeLayer,
        base_path: str,
    ):

        self.file_system_configuration = file_system_configuration
        self.base_path = base_path
        self.data_lake_layer = data_lake_layer
        self.logger = logging.getLogger(__name__)

    def load_ndjson(
        self,
        object_name: str,
        load_type: str,
        snapshot_timestamp: str,
        include_file_paths: str | None = None,
        schema_overrides: dict[str, pl.DataType] | None = None,
        schema: dict[str, pl.DataType] | None = None,
    ) -> pl.DataFrame:

        self.logger.info(
            "load_ndjson - Object name: %s, Type: %s, Snapshot timestamp: %s",
            object_name,
            load_type,
            snapshot_timestamp,
        )

        # If object name has .json suffix, strip it off
        if object_name.endswith(".json"):
            object_name = object_name[:-5]

        path = self._get_json_file_path(object_name, load_type, snapshot_timestamp)

        self.logger.info("load_ndjson - Target file path: %s", path)

        return pl.scan_ndjson(
            path,
            storage_options=self.file_system_configuration.storage_options,
            include_file_paths=include_file_paths,
            schema_overrides=schema_overrides,
            infer_schema_length=None,
            schema=schema,
        ).collect()

    def _get_json_file_path(self, object_name: str, load_type: str, snapshot_timestamp: str) -> str:
        path = self.file_system_configuration.get_full_path(
            self.data_lake_layer,
            f"{self.base_path}/{load_type}/snapshot_time={snapshot_timestamp}/{object_name}.json",
        )

        return path
