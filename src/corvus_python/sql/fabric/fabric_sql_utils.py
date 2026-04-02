import pyodbc
from ..sql_utils import get_pyodbc_connection


def get_fabric_sql_pyodbc_connection(
    sql_analytics_endpoint: str,
    database: str,
    use_managed_identity: bool = True,
    client_id: str | None = None,
) -> pyodbc.Connection:
    """Open a pyodbc connection to Fabric SQL Analytics endpoint using AAD tokens.

    Requires ODBC Driver 18 for SQL Server or later.

    Parameters
    ----------
    sql_analytics_endpoint:
        The Fabric SQL Analytics endpoint, e.g. "<id>.datawarehouse.fabric.microsoft.com".
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
    return get_pyodbc_connection(sql_analytics_endpoint, database, use_managed_identity, client_id)
