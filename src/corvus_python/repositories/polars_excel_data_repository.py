import logging
from typing import BinaryIO, cast
from io import BytesIO
import polars as pl
import fsspec
from opentelemetry import trace
from fastexcel import read_excel


from ..storage import StorageConfiguration, DataLakeLayer
from ..monitoring import all_methods_start_new_current_span_with_method_name

tracer = trace.get_tracer(__name__)


@all_methods_start_new_current_span_with_method_name(tracer)
class PolarsExcelDataRepository:
    def __init__(self, file_system_configuration: StorageConfiguration, data_lake_layer: DataLakeLayer, base_path: str):

        self.file_system_configuration = file_system_configuration
        self.base_path = base_path
        self.data_lake_layer = data_lake_layer
        self.logger = logging.getLogger(__name__)

    def load_excel(
        self, snapshot_timestamp: str, workbook_name: str, relative_path: str | None = None
    ) -> dict[str, pl.DataFrame]:

        self.logger.info("load_excel - Workbook name: %s, Snapshot timestamp: %s", workbook_name, snapshot_timestamp)

        path = self.get_file_path(workbook_name, snapshot_timestamp, relative_path)

        self.logger.info("load_excel - Target file path: %s", path)

        if (
            self.file_system_configuration.storage_options
            and self.file_system_configuration.storage_options.get("azure_storage_account_name", None) is not None
        ):
            self.logger.info(
                "load_excel - Using Azure storage account: %s",
                self.file_system_configuration.storage_options["azure_storage_account_name"],
            )

            storage_options = {
                "azure_storage_account_name": self.file_system_configuration.storage_options[
                    "azure_storage_account_name"
                ],
                "anon": False,
            }
        else:
            storage_options = self.file_system_configuration.storage_options or {}

        with fsspec.open(path, **storage_options) as f:
            f = cast(BinaryIO, f)
            workbook_bytes = f.read()

        worksheets = pl.read_excel(BytesIO(workbook_bytes), sheet_id=0, engine="calamine")

        return worksheets

    def get_file_path(self, workbook_name: str, snapshot_timestamp: str, relative_path: str | None = None) -> str:
        base_path = self.base_path if relative_path is None else f"{self.base_path}/{relative_path}"

        path = self.file_system_configuration.get_full_path(
            self.data_lake_layer, f"{base_path}/snapshot_time={snapshot_timestamp}/{workbook_name}.xlsx"
        )

        return path
