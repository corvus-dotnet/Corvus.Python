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
class TableInfo:
    """Dataclass to hold information about a table in a Synapse workspace.
    """
    database_name: str
    tables: list


def _get_sql_connection(workspace_name):
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
        table_infos: List[TableInfo],
        local_fs_base_path: str = os.path.join(os.getcwd(), "data")
        ):
    """Syncs tables from a Synapse workspace to a local Spark metastore.
    """

    conn = _get_sql_connection(workspace_name)

    file_system_configuration = LocalFileSystemStorageConfiguration(local_fs_base_path)
    spark = create_spark_session("table_syncer", file_system_configuration)

    for table_info in table_infos:
        _ = spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_info.database_name}")

        for table in table_info.tables:
            pdf = pd.read_sql(f'SELECT * FROM {table_info.database_name}.dbo.{table}', conn)
            spark.createDataFrame(pdf).coalesce(1).write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(f"{table_info.database_name}.{table}")
