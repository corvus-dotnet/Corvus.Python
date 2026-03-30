import polars as pl
import logging
from opentelemetry import trace

from ..storage import StorageConfiguration, DataLakeLayer
from ..monitoring import all_methods_start_new_current_span_with_method_name

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class PolarsCsvDataRepository:
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

    def load_csv(
        self, object_name: str, snapshot_timestamp: str, include_file_paths: str | None = None
    ) -> pl.DataFrame:

        self.logger.info(
            "load_csv - Object name: %s, Snapshot timestamp: %s",
            object_name,
            snapshot_timestamp,
        )

        # If object name has .csv suffix, strip it off
        if object_name.endswith(".csv"):
            object_name = object_name[:-4]

        path = self._get_csv_file_path(object_name, snapshot_timestamp)

        self.logger.info("load_csv - Target file path: %s", path)

        return pl.scan_csv(
            path, storage_options=self.file_system_configuration.storage_options, include_file_paths=include_file_paths
        ).collect()

    def _get_csv_file_path(self, object_name: str, snapshot_timestamp: str):
        path = self.file_system_configuration.get_full_path(
            self.data_lake_layer,
            f"{self.base_path}/snapshot_time={snapshot_timestamp}/{object_name}.csv",
        )

        return path
