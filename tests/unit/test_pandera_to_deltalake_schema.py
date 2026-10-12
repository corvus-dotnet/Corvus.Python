from datetime import date, datetime, timezone
from decimal import Decimal

import pandera.polars as pa
import polars as pl
import pytest
from deltalake import DeltaTable
from deltalake.schema import ArrayType, PrimitiveType, StructType
from deltalake.schema import Field as DeltaField
from polars.testing import assert_frame_equal

from corvus_python.schema import pandera_polars_to_deltalake_schema


def _delta_type(polars_type: pl.DataType, nullable: bool = True):
    schema = pa.DataFrameSchema({"value": pa.Column(polars_type, nullable=nullable)})
    return pandera_polars_to_deltalake_schema(schema).fields[0].type


@pytest.mark.parametrize(
    "polars_type, delta_type",
    [
        (pl.String, "string"),
        (pl.Int64, "long"),
        (pl.Int32, "integer"),
        (pl.Int16, "short"),
        (pl.Int8, "byte"),
        (pl.UInt32, "long"),
        (pl.UInt16, "integer"),
        (pl.UInt8, "short"),
        (pl.Float64, "double"),
        (pl.Float32, "float"),
        (pl.Boolean, "boolean"),
        (pl.Date, "date"),
        (pl.Binary, "binary"),
        (pl.Datetime("us", "UTC"), "timestamp"),
        (pl.Datetime("ms", "Europe/Paris"), "timestamp"),
        (pl.Datetime("us"), "timestamp_ntz"),
        (pl.Decimal(10, 2), "decimal(10,2)"),
        (pl.Decimal(scale=4), "decimal(38,4)"),
    ],
)
def test_primitive_types_map_to_their_delta_type(polars_type, delta_type):
    assert _delta_type(polars_type) == PrimitiveType(delta_type)


def test_nested_types_map_to_delta_structs_and_arrays_to_any_depth():
    polars_type = pl.Struct(
        {
            "name": pl.String,
            "amounts": pl.List(pl.Decimal(10, 2)),
            "games": pl.List(pl.Struct({"year": pl.Int16, "codes": pl.Array(pl.String, 2)})),
        }
    )

    assert _delta_type(polars_type) == StructType(
        [
            DeltaField("name", PrimitiveType("string"), nullable=True),
            DeltaField("amounts", ArrayType(PrimitiveType("decimal(10,2)"), contains_null=True), nullable=True),
            DeltaField(
                "games",
                ArrayType(
                    StructType(
                        [
                            DeltaField("year", PrimitiveType("short"), nullable=True),
                            DeltaField("codes", ArrayType(PrimitiveType("string"), contains_null=True), nullable=True),
                        ]
                    ),
                    contains_null=True,
                ),
                nullable=True,
            ),
        ]
    )


def test_columns_keep_their_name_and_nullability():
    schema = pa.DataFrameSchema(
        {"code": pa.Column(pl.String, nullable=False), "medals": pa.Column(pl.List(pl.Int32), nullable=True)}
    )

    fields = pandera_polars_to_deltalake_schema(schema).fields

    assert [(field.name, field.nullable) for field in fields] == [("code", False), ("medals", True)]


@pytest.mark.parametrize("polars_type", [pl.UInt64, pl.Categorical, pl.Object, pl.Duration("us")])
def test_types_with_no_delta_equivalent_are_rejected(polars_type):
    with pytest.raises(ValueError, match="Unsupported type"):
        _delta_type(polars_type)


def test_a_table_created_from_the_schema_accepts_data_the_schema_validates(tmp_path):
    schema = pa.DataFrameSchema(
        {
            "code": pa.Column(pl.String, nullable=False),
            "rank": pa.Column(pl.UInt8),
            "budget": pa.Column(pl.Decimal(10, 2)),
            "opened": pa.Column(pl.Date),
            "updated_at": pa.Column(pl.Datetime("us", "UTC")),
            "loaded_at": pa.Column(pl.Datetime("us")),
            "games": pa.Column(pl.List(pl.Struct({"year": pl.Int16, "medals": pl.List(pl.Int8)}))),
        }
    )
    df = schema.validate(
        pl.DataFrame(
            {
                "code": ["FRA"],
                "rank": [1],
                "budget": [Decimal("1234.50")],
                "opened": [date(1894, 1, 1)],
                "updated_at": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                "loaded_at": [datetime(2026, 1, 1)],
                "games": [[{"year": 2024, "medals": [16, 26, 22]}]],
            },
            schema={name: column.dtype.type for name, column in schema.columns.items()},
        )
    )
    path = str(tmp_path / "table")
    DeltaTable.create(path, pandera_polars_to_deltalake_schema(schema))

    df.write_delta(path, mode="append")

    # Delta has no unsigned integers, so the UInt8 column reads back as the Int16 it was stored as.
    assert_frame_equal(pl.read_delta(path), df.with_columns(pl.col("rank").cast(pl.Int16)))


def test_a_column_without_a_type_is_rejected():
    with pytest.raises(ValueError, match="column 'value' has no type"):
        pandera_polars_to_deltalake_schema(pa.DataFrameSchema({"value": pa.Column()}))
