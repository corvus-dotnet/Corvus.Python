from typing import Callable

import pyodbc
from corvus_python.sql import SelectColumn, create_or_alter_view_over_delta_table

from corvus_python.repositories import TableDefinition


def create_sql_views_from_tables(
    conn: pyodbc.Connection,
    schema_name: str,
    tables: list[TableDefinition],
    build_delta_table_path_func: Callable[[str], str],
):
    for table in tables:
        if table.schema is None:
            raise ValueError(f"Table {table.name} does not have a schema defined.")

        select_cols: list[SelectColumn] = []
        for name in table.schema.columns:
            title = table.schema.columns[name].title
            select_col = SelectColumn(name, title)
            select_cols.append(select_col)

        create_or_alter_view_over_delta_table(
            conn,
            schema_name,
            view_name=table.name,
            delta_table_path=build_delta_table_path_func(table.name),
            infer_types=True,
            select_columns=select_cols,
        )
