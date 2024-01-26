"""Copyright (c) Endjin Limited. All rights reserved."""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, expr


def null_safe_join(
        left_df: DataFrame,
        right_df: DataFrame,
        join_columns: List[tuple],
        left_cols_to_select: List[str],
        right_cols_to_select: List[str],
        join_how: str = 'left',
        broadcast_right_df: bool = False,
        left_alias: str = "left",
        right_alias: str = "right") -> DataFrame:

    """Performs a join between two DataFrames, using the specified join columns and null-safe equality.
    See https://spark.apache.org/docs/3.0.0-preview/sql-ref-null-semantics.html for more information on null
    semantics and the null-safe equality operator (<=>) in Spark."""

    # Using SQL syntax for the join conditions, which necessitates using the 'left' and 'right' aliases.
    # The <=> operator is used as it is a null-safe equality operator, and we can expect null values.
    left_df = left_df.alias(left_alias)
    right_df = right_df.alias(right_alias)
    join_conditions = [f"{left_alias}.`{j[0]}` <=> {right_alias}.`{j[1]}`" for j in join_columns]
    join_condition = " AND ".join(join_conditions)

    select_cols = [left_df[i] for i in left_cols_to_select] + [right_df[i] for i in right_cols_to_select]

    if broadcast_right_df:
        result_df = left_df.join(broadcast(right_df), expr(join_condition), join_how).select(*select_cols)
    else:
        result_df = left_df.join(right_df, expr(join_condition), join_how).select(*select_cols)

    return result_df
