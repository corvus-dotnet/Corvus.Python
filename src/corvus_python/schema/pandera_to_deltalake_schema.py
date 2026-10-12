import pandera.polars as pa
import polars as pl
from deltalake.schema import ArrayType, DataType, PrimitiveType, Schema, StructType
from deltalake.schema import Field as DeltaField

# Delta Lake has no unsigned integers, so each unsigned type maps to the next signed type
# wide enough to hold all of its values. UInt64 has none, so it isn't supported.
_PRIMITIVE_TYPES: dict[type[pl.DataType], str] = {
    pl.String: "string",
    pl.Int64: "long",
    pl.Int32: "integer",
    pl.Int16: "short",
    pl.Int8: "byte",
    pl.UInt32: "long",
    pl.UInt16: "integer",
    pl.UInt8: "short",
    pl.Float64: "double",
    pl.Float32: "float",
    pl.Boolean: "boolean",
    pl.Date: "date",
    pl.Binary: "binary",
}


def pandera_polars_to_deltalake_schema(schema: pa.DataFrameSchema) -> Schema:
    """Converts a Pandera Polars schema to a Delta Lake schema.

    Columns may be nested: a `pl.Struct` becomes a Delta struct, and a `pl.List` or `pl.Array`
    a Delta array, to any depth. A nested field, or a list's element, is nullable, since a Polars
    type doesn't say whether it may be null; only a column's own nullability comes from the schema.

    A `pl.Decimal` keeps its precision and scale. A `pl.Datetime` with a time zone becomes a Delta
    `timestamp`, and one without a time zone a `timestamp_ntz`, which is the type Polars writes
    for it.

    Args:
        schema: A Pandera Polars schema object.

    Returns:
        Schema: A Delta Lake schema object representing the schema.

    Raises:
        ValueError: If a field type is not supported.
    """
    return Schema(
        [
            DeltaField(
                name=column.name,
                type=_delta_type(column.dtype.type if column.dtype is not None else None, column.name),
                nullable=column.nullable,
                metadata={},
            )
            for column in schema.columns.values()
        ]
    )


def _delta_type(polars_type: pl.DataType | None, column_name: str) -> DataType:
    if polars_type is None:
        raise ValueError(f"Unsupported type: column '{column_name}' has no type")
    if isinstance(polars_type, pl.Datetime):
        return PrimitiveType("timestamp" if polars_type.time_zone else "timestamp_ntz")
    if isinstance(polars_type, pl.Decimal):
        # Polars leaves a decimal's precision unset to mean its maximum, which is 38.
        return PrimitiveType(f"decimal({polars_type.precision or 38},{polars_type.scale})")
    if isinstance(polars_type, (pl.List, pl.Array)):
        return ArrayType(_delta_type(polars_type.inner, column_name), contains_null=True)
    if isinstance(polars_type, pl.Struct):
        return StructType(
            [
                DeltaField(name=field.name, type=_delta_type(field.dtype, column_name), nullable=True)
                for field in polars_type.fields
            ]
        )
    primitive = _PRIMITIVE_TYPES.get(polars_type.base_type())
    if primitive is None:
        raise ValueError(f"Unsupported type: {polars_type} in column '{column_name}'")
    return PrimitiveType(primitive)
