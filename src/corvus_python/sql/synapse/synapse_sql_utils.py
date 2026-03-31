import pyodbc
from ..sql_utils import (
    get_pyodbc_connection,
    get_pyodbc_connection_with_token,
)


def get_synapse_sql_pyodbc_connection(
    workspace_name: str,
    database: str,
    use_managed_identity: bool = True,
    client_id: str | None = None,
) -> pyodbc.Connection:
    """Open a pyodbc connection to Synapse serverless SQL endpoint using AAD tokens.

    Requires ODBC Driver 18 for SQL Server or later.

    Parameters
    ----------
    workspace_name:
        The Synapse workspace name, e.g. "myworkspace".
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
    server = f"{workspace_name}-ondemand.sql.azuresynapse.net"
    return get_pyodbc_connection(server, database, use_managed_identity, client_id)


def get_synapse_sql_pyodbc_connection_with_token(
    workspace_name: str,
    database: str,
    token: str,
) -> pyodbc.Connection:
    """Open a pyodbc connection to Synapse serverless SQL endpoint using a pre-acquired AAD token.

    Requires ODBC Driver 18 for SQL Server or later.

    Parameters
    ----------
    workspace_name:
        The Synapse workspace name, e.g. "myworkspace".
    database:
        The database name.
    token:
        A pre-acquired AAD token with audience "https://database.windows.net/.default".
    """
    server = f"{workspace_name}-ondemand.sql.azuresynapse.net"
    return get_pyodbc_connection_with_token(server, database, token)


def create_database_if_not_exists(
    conn: pyodbc.Connection,
    database_name: str,
) -> None:
    """Create a database if it doesn't already exist."""

    if conn.getinfo(pyodbc.SQL_DATABASE_NAME) != "master":
        raise ValueError("Connection must be to master database to create a new database.")

    prev_autocommit = conn.autocommit
    conn.autocommit = True

    ddl = f"""
    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database_name}')
    BEGIN
        CREATE DATABASE [{database_name}]
    END
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(ddl)
    finally:
        conn.autocommit = prev_autocommit
