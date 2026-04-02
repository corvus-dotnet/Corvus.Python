import pandera.polars as pa
from deltalake.schema import Field as DeltaField
from deltalake.schema import PrimitiveType, Schema


def pandera_polars_to_deltalake_schema(schema: pa.DataFrameSchema) -> Schema:
    """Converts a Pandera Polars schema to a Delta Lake schema.

    Args:
        schema: A Pandera Polars schema object.

    Returns:
        Schema: A Delta Lake schema object representing the schema.

    Raises:
        ValueError: If a field type is not supported.
    """
    # Mapping of Polars types to Delta Lake PrimitiveType
    type_mapping: dict[str, PrimitiveType] = {
        "Utf8": PrimitiveType("string"),
        "String": PrimitiveType("string"),
        "Int64": PrimitiveType("long"),
        "Int32": PrimitiveType("integer"),
        "Float64": PrimitiveType("double"),
        "Float32": PrimitiveType("float"),
        "Boolean": PrimitiveType("boolean"),
        "Date": PrimitiveType("date"),
        # Note: Datetime types are handled separately below with startswith check
    }

    delta_fields: list[DeltaField] = []

    for col in schema.columns.values():
        column: pa.Column = col

        field_name = column.name
        polars_type = column.dtype

        # Get the Delta Lake type
        polars_type_str = str(polars_type)

        if polars_type_str.startswith("Datetime"):
            # All datetime types map to Delta Lake timestamp
            delta_type = PrimitiveType("timestamp")
        elif polars_type_str.startswith("Decimal"):
            delta_type = PrimitiveType("double")
        else:
            delta_type: PrimitiveType | None = type_mapping.get(polars_type_str)
            if delta_type is None:
                raise ValueError(f"Unsupported type: {polars_type_str}")

        # Create the Delta Lake field
        delta_field: DeltaField = DeltaField(
            name=field_name,
            type=delta_type,
            nullable=col.nullable,
            metadata={},
        )

        delta_fields.append(delta_field)

    return Schema(delta_fields)
