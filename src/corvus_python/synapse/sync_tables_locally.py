"""Copyright (c) Endjin Limited. All rights reserved."""

from dataclasses import dataclass
from typing import List
from azure.identity import AzureCliCredential, CredentialUnavailableError
import pyodbc
import struct
import pandas as pd
import os

from corvus_python.pyspark.storage import LocalFileSystemStorageConfiguration
from corvus_python.pyspark.utilities import create_spark_session


@dataclass
class ObjectSyncDetails:
    """Holds information about objects to sync from a Synapse workspace.

    Attributes:
        database_name (str): Name of the database.
        tables (List[str]): List of tables in the database.
    """
    database_name: str
    tables: List[str]


def _get_sql_connection(workspace_name: str) -> pyodbc.Connection:
    """Gets an ODBC connection to the SQL Serverless endpoint of a Synapse workspace.

    Args:
        workspace_name (str): Name of the workspace.

    Returns:
        pyodbc.Connection: ODBC connection to the SQL Serverless endpoint.

    Raises:
        RuntimeError: If user is not logged into the Azure CLI.
    """
    server = f'{workspace_name}-ondemand.sql.azuresynapse.net'
    database = 'master'
    driver = '{ODBC Driver 17 for SQL Server}'
    connection_string = f'Driver={driver};Server=tcp:{server},1433;Database={database};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'  # noqa: E501

    credential = AzureCliCredential()

    try:
        token_bytes = credential.get_token('https://database.windows.net/.default').token.encode("UTF-16-LE")
    except CredentialUnavailableError:
        raise RuntimeError("Please login to the Azure CLI using `az login --tenant <tenant> "
                           "--use-device-code` to authenticate.")

    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    conn = pyodbc.connect(connection_string, attrs_before={1256: token_struct})
    return conn


def sync_tables_to_local_spark(
        workspace_name: str,
        table_infos: List[ObjectSyncDetails],
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
            Defaults to False.
    """

    conn = _get_sql_connection(workspace_name)

    file_system_configuration = LocalFileSystemStorageConfiguration(local_fs_base_path)
    spark = create_spark_session("table_syncer", file_system_configuration)

    for table_info in table_infos:
        _ = spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_info.database_name}")

        for table in table_info.tables:
            table_exists = spark.catalog.tableExists(table, table_info.database_name)

            if table_exists and not overwrite:
                print('\033[93m' + f"Table '{table}' in database '{table_info.database_name}' already exists and overwrite is set to False. Skipping table sync." + '\033[0m')  # noqa: E501
                continue
            else:
                pdf = pd.read_sql(f'SELECT * FROM {table_info.database_name}.dbo.{table}', conn)
                spark.createDataFrame(pdf).coalesce(1).write \
                    .format("delta") \
                    .mode("overwrite") \
                    .saveAsTable(f"{table_info.database_name}.{table}")

    conn.close()
    spark.stop()
