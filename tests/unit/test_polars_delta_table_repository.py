import warnings

import pandera.polars as pa
import polars as pl
import pytest
from deltalake import DeltaTable
from polars.testing import assert_frame_equal

from corvus_python.repositories import DatabaseDefinition, PolarsDeltaTableRepository, TableDefinition
from corvus_python.schema import pandera_polars_to_deltalake_schema
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


def _schema(columns: dict[str, tuple[pl.DataType, bool]]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {name: pa.Column(dtype, nullable=nullable) for name, (dtype, nullable) in columns.items()}
    )


def _frame(columns: dict[str, tuple[pl.DataType, bool]], rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema={name: dtype for name, (dtype, _) in columns.items()}, orient="row")


def _delta_schema(repository):
    path = repository._get_table_path("medals")
    return [(field.name, field.type, field.nullable) for field in DeltaTable(path).schema().fields]


def _declared_schema(schema: pa.DataFrameSchema):
    return [(field.name, field.type, field.nullable) for field in pandera_polars_to_deltalake_schema(schema).fields]


NOT_NULL = {"code": (pl.String, False), "value": (pl.Int32, False)}
NULLABLE_VALUE = {"code": (pl.String, False), "value": (pl.Int32, True)}


@pytest.mark.parametrize(
    "before, after, rows",
    [
        pytest.param(NOT_NULL, NULLABLE_VALUE, [("FRA", None)], id="not null to nullable"),
        pytest.param(NULLABLE_VALUE, NOT_NULL, [("FRA", 2)], id="nullable to not null"),
        pytest.param(NOT_NULL, {**NOT_NULL, "value": (pl.Int64, False)}, [("FRA", 3_000_000_000)], id="int to long"),
        pytest.param(NOT_NULL, {**NOT_NULL, "note": (pl.String, False)}, [("FRA", 2, "n")], id="add a not null column"),
        pytest.param(NOT_NULL, {"code": (pl.String, False)}, [("FRA",)], id="remove a column"),
    ],
)
def test_overwriting_the_schema_changes_an_existing_table_to_its_definitions_schema(tmp_path, before, after, rows):
    _repository(tmp_path, _schema(before)).overwrite_table(
        "medals", _frame(before, [("FRA", 1)]), overwrite_schema=True
    )
    repository = _repository(tmp_path, _schema(after))

    repository.overwrite_table("medals", _frame(after, rows), overwrite_schema=True)

    assert _delta_schema(repository) == _declared_schema(_schema(after))
    assert _read(repository).rows() == rows


def test_overwriting_with_a_condition_changes_the_whole_tables_schema(tmp_path):
    after = {"code": (pl.String, False), "value": (pl.Int64, True)}
    _repository(tmp_path, _schema(NOT_NULL)).overwrite_table(
        "medals", _frame(NOT_NULL, [("FRA", 1), ("GBR", 2)]), overwrite_schema=True
    )
    repository = _repository(tmp_path, _schema(after))

    repository.overwrite_table_with_condition(
        "medals", _frame(after, [("FRA", None)]), "code = 'FRA'", overwrite_schema=True
    )

    assert _delta_schema(repository) == _declared_schema(_schema(after))
    assert _read(repository).rows() == [("FRA", None), ("GBR", 2)]


def test_a_lazy_frame_is_written_with_every_value_validated(tmp_path):
    repository = _repository(tmp_path, _schema(NOT_NULL))

    with pytest.raises(pa.errors.SchemaError):
        repository.overwrite_table("medals", _frame(NULLABLE_VALUE, [("FRA", None)]).lazy(), overwrite_schema=True)

    repository.overwrite_table("medals", _frame(NOT_NULL, [("FRA", 1)]).lazy(), overwrite_schema=True)
    assert _delta_schema(repository) == _declared_schema(_schema(NOT_NULL))
