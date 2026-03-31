from dataclasses import dataclass
from typing import List

import pyodbc
import struct
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential


def execute_ddl(
    conn: pyodbc.Connection,
    ddl: str,
) -> None:
    """Execute a DDL statement (e.g. CREATE VIEW) using a pyodbc connection."""
    with conn.cursor() as cursor:
        cursor.execute(ddl)
        conn.commit()


def get_pyodbc_connection(
    server: str,
    database: str,
    use_managed_identity: bool = True,
    client_id: str | None = None,
) -> pyodbc.Connection:
    """Open a pyodbc connection to Synapse serverless SQL or Fabric SQL Analytics endpoint using AAD tokens.

    Requires ODBC Driver 18 for SQL Server or later.

    Parameters
    ----------
    server:
        The Synapse serverless endpoint or Fabric SQL Analytics endpoint,
        e.g. "myworkspace-ondemand.sql.azuresynapse.net"
            or "<id>.datawarehouse.fabric.microsoft.com".
    database:
        The database name.
    use_managed_identity:
        If True, uses ManagedIdentityCredential (for Container Apps / ACI).
        If False, uses DefaultAzureCredential (for local dev — picks up
        az login, VS Code, environment variables, etc.).
    client_id:
        Optional user-assigned managed identity client ID. Only used when
        use_managed_identity is True.
    """
    # 1. Acquire an AAD token for the Azure SQL resource
    resource = "https://database.windows.net/.default"

    if use_managed_identity:
        credential = ManagedIdentityCredential(client_id=client_id)
    else:
        credential = DefaultAzureCredential()

    token = credential.get_token(resource)

    # 2. Pack the token into the bytes format pyodbc expects for
    #    SQL_COPT_SS_ACCESS_TOKEN (driver-level AAD token injection).
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    # 3. Build the connection string — no UID/PWD needed
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"ENCRYPT=yes;"
        f"TrustServerCertificate=no;"
    )

    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    return conn


@dataclass
class SelectColumn:
    name: str
    title: str | None = None


@dataclass
class WithColumn:
    name: str
    type: str
    title: str | None = None


def generate_create_or_alter_view_over_delta_table_ddl(
    schema_name: str,
    view_name: str,
    delta_table_path: str,
    infer_types: bool = True,
    select_columns: List[SelectColumn] | None = None,
    with_columns: List[WithColumn] | None = None,
) -> str:
    """
    Generate the DDL statement to create or alter a SQL view over a Delta Lake table.


    Parameters
    ----------
    schema_name:
        The schema for the view, e.g. "dbo".
    view_name:
        The name of the view to create or alter.
    delta_table_path:
        The path to the Delta Lake table, e.g. "abfss://container@storageaccount.dfs.core.windows.net/path/to/table".
    infer_types:
        If True, the view will be created with "SELECT [col] AS [title],..." and Synapse will infer the column types
        from the data.
        If False, the view will be created with "SELECT * FROM OPENROWSET(...) WITH ([col] type, ...)" and the column
        types will be determined by the caller and passed in via with_columns.
    select_columns:
        If infer_types is True, the list of columns to select from the Delta table, along with optional titles for the
        view. If a title is not provided for a column, the original column name will be used in the view.
    with_columns:
        If infer_types is False, the list of columns and their types to define in the WITH clause of the OPENROWSET
        statement. The column names should match the columns in the Delta table, and the types should be valid
        Synapse SQL types (e.g. "INT", "VARCHAR(100)", "DATETIME2", etc.).
    """

    if infer_types:
        if not select_columns:
            raise ValueError("select_columns must be provided when infer_types is True")

        col_def_list: list[str] = []
        for column in select_columns:
            if column.title:
                col_def_list.append(f"[{column.name}] AS [{column.title}]")
            else:
                col_def_list.append(f"[{column.name}]")

        select_clause = ",\n        ".join(col_def_list)
        with_clause = ""
    else:
        select_clause = "*"
        with_column_list: list[str] = []
        if not with_columns:
            raise ValueError("with_columns must be provided when infer_types is False")
        for column in with_columns:
            with_column_list.append(f"[{column.title or column.name}] {column.type} '$.{column.name}'")
        with_clause = ",\n    ".join(with_column_list)
        with_clause = f"\n    WITH (\n        {with_clause}\n    )"

    bulk_clause = f"BULK '{delta_table_path}',\n" f"FORMAT = 'DELTA'"

    ddl = f"""\
CREATE OR ALTER VIEW [{schema_name}].[{view_name}]
AS
SELECT
    {select_clause}
FROM OPENROWSET(
        {bulk_clause}
    ){with_clause} AS [result]"""

    return ddl


def create_or_alter_view_over_delta_table(
    conn: pyodbc.Connection,
    schema_name: str,
    view_name: str,
    delta_table_path: str,
    infer_types: bool = True,
    select_columns: List[SelectColumn] | None = None,
    with_columns: List[WithColumn] | None = None,
) -> None:
    """
    Create or alter a SQL view over a Delta Lake table.


    Parameters
    ----------
    conn:
        An open pyodbc connection to the Synapse workspace.
    schema_name:
        The schema for the view, e.g. "dbo".
    view_name:
        The name of the view to create or alter.
    delta_table_path:
        The path to the Delta Lake table, e.g. "abfss://container@storageaccount.dfs.core.windows.net/path/to/table".
    infer_types:
        If True, the view will be created with "SELECT [col] AS [title],..." and Synapse will infer the column types
        from the data.
        If False, the view will be created with "SELECT * FROM OPENROWSET(...) WITH ([col] type, ...)" and the column
        types will be determined by the caller and passed in via with_columns.
    select_columns:
        If infer_types is True, the list of columns to select from the Delta table, along with optional titles for the
        view. If a title is not provided for a column, the original column name will be used in the view.
    with_columns:
        If infer_types is False, the list of columns and their types to define in the WITH clause of the OPENROWSET
        statement. The column names should match the columns in the Delta table, and the types should be valid
        Synapse SQL types (e.g. "INT", "VARCHAR(100)", "DATETIME2", etc.).
    """

    ddl = generate_create_or_alter_view_over_delta_table_ddl(
        schema_name, view_name, delta_table_path, infer_types, select_columns, with_columns
    )

    execute_ddl(conn, ddl)


def drop_views_in_schema(
    conn: pyodbc.Connection,
    schema_name: str = "dbo",
) -> None:
    """
    Drop all views in a given schema.

    Parameters
    ----------
    conn:
        An open pyodbc connection to the Synapse workspace.
    schema_name:
        The schema from which to drop all views, e.g. "dbo".
    """
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = '{schema_name}'
        """
    )
    views = cursor.fetchall()
    for view in views:
        view_name = view[0]
        drop_ddl = f"DROP VIEW [{schema_name}].[{view_name}]"
        execute_ddl(conn, drop_ddl)
