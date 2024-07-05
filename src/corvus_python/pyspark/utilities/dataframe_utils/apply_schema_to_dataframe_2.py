from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode


def apply_schema_to_dataframe(df: DataFrame, schema: StructType) -> DataFrame:
    def apply_schema_recursively(df: DataFrame, schema: StructType, parent_path: str = "") -> DataFrame:
        for field in schema.fields:
            current_path = f"{parent_path}.{field.name}" if parent_path else field.name

            if isinstance(field.dataType, StructType):
                # Handle nested StructType
                for nested_field in field.dataType.fields:
                    nested_path = f"{current_path}.{nested_field.name}"
                    df = df.withColumn(nested_path, col(nested_path).cast(nested_field.dataType))
                df = apply_schema_recursively(df, field.dataType, current_path)
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                # Handle ArrayType of StructType by exploding the array
                df = df.withColumn(current_path, explode(col(current_path)))
                df = apply_schema_recursively(df, field.dataType.elementType, current_path)
            else:
                # Handle simple types
                if parent_path:
                    # If there's a parent path, it means we're working with a nested field
                    df = df.withColumn(current_path, col(current_path).cast(field.dataType))
                else:
                    # For top-level fields, directly apply the cast
                    df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        return df

    result_df = apply_schema_recursively(df, schema)

    # Reorder columns to match the schema
    ordered_columns = [col(field.name) for field in schema.fields]
    result_df = result_df.select(*ordered_columns)

    return result_df


def flatten_schema(schema: StructType, prefix: str = '') -> list:
    """
    Flatten the schema and return list of dot-separated column names.
    """
    fields = []
    for field in schema.fields:
        if isinstance(field.dataType, StructType):
            fields += flatten_schema(field.dataType, prefix + field.name + '.')
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            # For ArrayType with StructType elements, handle similarly but indicate array with []
            fields += flatten_schema(field.dataType.elementType, prefix + field.name + '[]' + '.')
        else:
            fields.append(prefix + field.name)
    return fields


def select_in_target_order(df: DataFrame, target_schema: StructType) -> DataFrame:
    """
    Reorder DataFrame columns to match the target schema, including nested structures.
    """
    # Flatten the target schema to get the target order of columns
    target_columns_order = flatten_schema(target_schema)

    # Generate a mapping of flat column name to its DataFrame column expression
    # This includes handling nested structures and arrays
    def generate_column_exprs(schema: StructType, prefix: str = '') -> dict:
        exprs = {}
        for field in schema.fields:
            col_name = prefix + field.name
            if isinstance(field.dataType, StructType):
                exprs.update(generate_column_exprs(field.dataType, col_name + '.'))
            elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
                # For arrays of structs, use the explode function to flatten, then handle the nested struct
                exprs.update(generate_column_exprs(field.dataType.elementType, col_name + '[]' + '.'))
            else:
                exprs[col_name] = col(col_name.replace('[]', ''))
        return exprs

    # Generate column expressions for the current DataFrame schema
    column_exprs = generate_column_exprs(df.schema)

    # Reorder columns based on the target schema
    reordered_columns = [column_exprs[col_name] for col_name in target_columns_order if col_name in column_exprs]

    return df.select(*reordered_columns)
