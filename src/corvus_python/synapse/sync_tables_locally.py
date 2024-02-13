"""Copyright (c) Endjin Limited. All rights reserved."""

from dataclasses import dataclass
from typing import List, Tuple
import pyodbc
import struct
import os

from corvus_python.pyspark.storage import LocalFileSystemStorageConfiguration
from corvus_python.pyspark.utilities import create_spark_session
from corvus_python.auth import get_az_cli_token


@dataclass
class ObjectSyncDetails:
    """Holds information about objects to sync from a Synapse workspace.

    Attributes:
        database_name (str): Name of the database.
        tables (List[str]): List of tables in the database.
    """
    database_name: str
    tables: List[str]


def _get_pyodbc_connection(workspace_name: str) -> pyodbc.Connection:
    """Gets an ODBC connection to the SQL Serverless endpoint of a Synapse workspace.

    Args:
        workspace_name (str): Name of the workspace.

    Returns:
        pyodbc.Connection: ODBC connection to the SQL Serverless endpoint.

    Raises:
        RuntimeError: If user is not logged into the Azure CLI.

    Note:
        The connection object returned can be used in a pandas read_sql query to pull data from Synapse. E.g.:
        df = pd.read_sql(f'SELECT * FROM db_name.dbo.table_name', conn)
    """
    server = f'{workspace_name}-ondemand.sql.azuresynapse.net'
    database = 'master'
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'Driver={driver};Server=tcp:{server},1433;\
        Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    token = get_az_cli_token('https://database.windows.net/.default')

    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    conn = pyodbc.connect(connection_string, attrs_before={1256: token_struct})
    return conn


def _get_jdbc_connection_properties(workspace_name: str) -> Tuple[str, dict]:
    """Gets the Spark JDBC connection properties for a Synapse SQL Serverless endpoint.

    Args:
        workspace_name (str): Name of the workspace.

    Returns:
        Tuple[str, dict]: Tuple containing the JDBC URL and connection properties.

    Raises:
        RuntimeError: If user is not logged into the Azure CLI.
    """
    server = f'{workspace_name}-ondemand.sql.azuresynapse.net'
    database = 'master'

    jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;\
        trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

    token = get_az_cli_token('https://database.windows.net/.default')

    connection_properties = {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "accessToken": token
    }

    return jdbc_url, connection_properties


def sync_synapse_tables_to_local_spark(
        workspace_name: str,
        object_sync_details: List[ObjectSyncDetails],
        local_fs_base_path: str = os.path.join(os.getcwd(), "data"),
        overwrite: bool = False
        ):
    """Syncs tables from a Synapse workspace to a local Spark metastore.

    Args:
        workspace_name (str): Name of the Synapse workspace.
        table_infos (List[ObjectSyncDetails]): List of ObjectSyncDetails containing info about the tables to sync.
        local_fs_base_path (str, optional): Base path for the local file system.
            Defaults to os.path.join(os.getcwd(), "data").
        overwrite (bool, optional): Whether to overwrite the tables if they already exist in the local metastore.
            Defaults to False, to avoid unnecessary pulling of data from Synapse (and therefore egress charges).
    """

    file_system_configuration = LocalFileSystemStorageConfiguration(local_fs_base_path)
    spark = create_spark_session("table_syncer", file_system_configuration)

    jdbc_url, connection_properties = _get_jdbc_connection_properties(workspace_name)

    for osd in object_sync_details:
        _ = spark.sql(f"CREATE DATABASE IF NOT EXISTS {osd.database_name}")

        for table in osd.tables:
            table_exists = spark.catalog.tableExists(table, osd.database_name)

            if table_exists and not overwrite:
                print('\033[93m' + f"Table '{table}' in database '{osd.database_name}' already exists and\
                       overwrite is set to False. Skipping table sync." + '\033[0m')
                continue
            else:
                spark.read.jdbc(
                        url=jdbc_url,
                        table=f"{osd.database_name}.dbo.{table}",
                        properties=connection_properties
                    ) \
                    .coalesce(1) \
                    .write \
                    .format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(f"{osd.database_name}.{table}")

    spark.stop()
