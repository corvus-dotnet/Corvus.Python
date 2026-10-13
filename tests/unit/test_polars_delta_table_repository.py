import warnings

import pandera.polars as pa
import polars as pl
import pytest
from deltalake import DeltaTable
from polars.testing import assert_frame_equal

from corvus_python.repositories import DatabaseDefinition, PolarsDeltaTableRepository, TableDefinition
from corvus_python.storage import DataLakeLayer, LocalFileSystemStorageConfiguration

SCHEMA = pa.DataFrameSchema({"code": pa.Column(pl.String), "value": pa.Column(pl.Int64)})
WIDER_SCHEMA = pa.DataFrameSchema({"code": pa.Column(pl.String), "value": pa.Column(pl.Float64)})


def _repository(tmp_path, schema=SCHEMA, configuration=None) -> PolarsDeltaTableRepository:
    return PolarsDeltaTableRepository(
        configuration or LocalFileSystemStorageConfiguration(str(tmp_path)),
        DataLakeLayer.SILVER,
        "workload",
        DatabaseDefinition("sr__workload", [TableDefinition("medals", schema)]),
    )


def _read(repository) -> pl.DataFrame:
    return repository.read_data("medals").collect().sort("code")


def test_overwriting_replaces_the_rows_without_a_deprecation_warning(tmp_path):
    repository = _repository(tmp_path)
    repository.overwrite_table("medals", pl.DataFrame({"code": ["FRA", "GBR"], "value": [1, 2]}))

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        repository.overwrite_table("medals", pl.DataFrame({"code": ["USA"], "value": [3]}))

    assert_frame_equal(_read(repository), pl.DataFrame({"code": ["USA"], "value": [3]}))


def test_overwriting_the_schema_replaces_a_columns_type(tmp_path):
    _repository(tmp_path).overwrite_table("medals", pl.DataFrame({"code": ["FRA"], "value": [1]}))
    repository = _repository(tmp_path, WIDER_SCHEMA)

    repository.overwrite_table("medals", pl.DataFrame({"code": ["FRA"], "value": [1.5]}), overwrite_schema=True)

    assert_frame_equal(_read(repository), pl.DataFrame({"code": ["FRA"], "value": [1.5]}))


def test_overwriting_lazily_the_schema_replaces_a_columns_type(tmp_path):
    _repository(tmp_path).overwrite_table("medals", pl.DataFrame({"code": ["FRA"], "value": [1]}))
    repository = _repository(tmp_path, WIDER_SCHEMA)

    repository.overwrite_table_lazy("medals", pl.LazyFrame({"code": ["FRA"], "value": [1.5]}), overwrite_schema=True)

    assert_frame_equal(_read(repository), pl.DataFrame({"code": ["FRA"], "value": [1.5]}))


def test_overwriting_with_a_condition_replaces_only_the_rows_it_matches(tmp_path):
    repository = _repository(tmp_path)
    repository.overwrite_table("medals", pl.DataFrame({"code": ["FRA", "GBR"], "value": [1, 2]}))

    repository.overwrite_table_with_condition("medals", pl.DataFrame({"code": ["FRA"], "value": [9]}), "code = 'FRA'")

    assert_frame_equal(_read(repository), pl.DataFrame({"code": ["FRA", "GBR"], "value": [9, 2]}))


def test_overwriting_with_a_condition_validates_the_data_against_the_schema(tmp_path):
    repository = _repository(tmp_path)
    repository.overwrite_table("medals", pl.DataFrame({"code": ["FRA"], "value": [1]}))

    with pytest.raises(pa.errors.SchemaError):
        repository.overwrite_table_with_condition("medals", pl.DataFrame({"code": ["FRA"]}), "code = 'FRA'")


def test_storage_options_are_read_from_the_configuration_each_time_theyre_used(tmp_path):
    configuration = LocalFileSystemStorageConfiguration(str(tmp_path))
    repository = _repository(tmp_path, configuration=configuration)

    configuration.storage_options = {"token": "renewed"}

    assert repository.storage_options == {"token": "renewed"}


NULLABLE_VALUE_SCHEMA = pa.DataFrameSchema(
    {
        "code": pa.Column(pl.String),
        "value": pa.Column(pl.Int64, nullable=True),
        "detail": pa.Column(pl.Struct({"note": pl.String}), nullable=True),
    }
)


def _nullability(repository) -> dict[str, bool]:
    path = repository._get_table_path("medals")
    return {field.name: field.nullable for field in DeltaTable(path).schema().fields}


def _row(code, value):
    return pl.DataFrame(
        {"code": [code], "value": [value], "detail": [{"note": "n"}]},
        schema={"code": pl.String, "value": pl.Int64, "detail": pl.Struct({"note": pl.String})},
    )


def test_overwriting_the_schema_keeps_the_columns_the_definition_declares_not_nullable(tmp_path):
    repository = _repository(tmp_path, NULLABLE_VALUE_SCHEMA)

    repository.overwrite_table("medals", _row("FRA", 1), overwrite_schema=True)

    assert _nullability(repository) == {"code": False, "value": True, "detail": True}


def test_appending_and_overwriting_with_a_condition_keep_the_columns_not_nullable(tmp_path):
    repository = _repository(tmp_path, NULLABLE_VALUE_SCHEMA)
    repository.overwrite_table("medals", _row("FRA", 1), overwrite_schema=True)

    repository.append_to_table("medals", _row("GBR", None))
    repository.overwrite_table_with_condition("medals", _row("FRA", 2), "code = 'FRA'", overwrite_schema=True)

    assert _nullability(repository) == {"code": False, "value": True, "detail": True}
    assert _read(repository).select("code", "value").rows() == [("FRA", 2), ("GBR", None)]
