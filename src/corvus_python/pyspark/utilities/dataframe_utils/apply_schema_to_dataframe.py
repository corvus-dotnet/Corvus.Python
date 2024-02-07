"""Copyright (c) Endjin Limited. All rights reserved."""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession


def _generate_select_expression(schema: StructType, parent_field_name: str = None):
    select_expressions = []
    
    for field in schema.fields:
        select_expr = ""
        if field.typeName() == "struct":
            _generate_select_expression(field, parent_field_name)
        # elif field.typeName() == "array":
        #     select_expr.append(f"explode({field.metadata.get('source_col', field.name)}) as {field.name}")
        else:
            source_col = field.metadata.get('source_col', field.name)
            select_expr = f"{source_col} as {field.name}"

            if parent_field_name:
                select_expr = f"{parent_field_name}.{select_expr}"
            else:
                select_expr + f"{field.metadata.get('source_col', field.name)} as {field.name}")
            select_expressions.append(select_expr)

    return 

def apply_schema_to_dataframe(sprak: SparkSession, df: DataFrame, schema: StructType):
    select_expr = _generate_select_expression(schema)

    rdd = df.rdd
    return spark.createDataFrame(rdd, schema)


