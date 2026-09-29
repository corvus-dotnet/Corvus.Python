import os
import shutil

import pytest

from corvus_python.repositories import PolarsExcelDataRepository
from corvus_python.storage import DataLakeLayer, FabricLakehouseFilesConfiguration, LocalFileSystemStorageConfiguration

WORKBOOK = os.path.join(os.path.dirname(__file__), "test_data", "polars_excel_data_repository", "games.xlsx")


@pytest.fixture
def repository(tmp_path):
    """Lays the workbook out as bronze/excel/snapshot_time=<ts>/games.xlsx under a local configuration."""
    snapshot = tmp_path / "bronze" / "excel" / "snapshot_time=20260929"
    snapshot.mkdir(parents=True)
    shutil.copy(WORKBOOK, snapshot / "games.xlsx")
    return PolarsExcelDataRepository(LocalFileSystemStorageConfiguration(str(tmp_path)), DataLakeLayer.BRONZE, "excel")


def test_load_excel_returns_every_sheet(repository):
    sheets = repository.load_excel("20260929", "games")

    assert sorted(sheets) == ["Games", "Types"]
    assert sheets["Games"]["name"].to_list() == ["Chess", "Go"]
    assert sheets["Types"]["type"].to_list() == ["Board"]


def test_load_excel_raises_file_not_found_for_missing_workbook(repository):
    with pytest.raises(FileNotFoundError):
        repository.load_excel("20260929", "missing")


def test_load_excel_raises_file_not_found_for_missing_snapshot(repository):
    with pytest.raises(FileNotFoundError):
        repository.load_excel("19990101", "games")


def test_get_file_path_resolves_through_a_fabric_files_configuration():
    config = FabricLakehouseFilesConfiguration("ws")
    repository = PolarsExcelDataRepository(config, DataLakeLayer.BRONZE, "excel")

    assert (
        repository.get_file_path("games", "20260929")
        == "abfss://ws@onelake.dfs.fabric.microsoft.com/bronze.Lakehouse/Files/excel/snapshot_time=20260929/games.xlsx"
    )
