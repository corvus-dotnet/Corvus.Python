"""Copyright (c) Endjin Limited. All rights reserved."""

from dataclasses import dataclass
from typing import List, Tuple
from pyspark.sql import SparkSession

from corvus_python.pyspark.utilities import LocalSparkSessionConfig, get_or_create_spark_session
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


def _get_jdbc_connection_properties(workspace_name: str) -> Tuple[str, dict]:
    """Gets the Spark JDBC connection properties for a Synapse SQL Serverless endpoint.

    Args:
        workspace_name (str): Name of the workspace.

    Returns:
        Tuple[str, dict]: Tuple containing the JDBC URL and connection properties.

    Raises:
        RuntimeError: If user is not logged into the Azure CLI.
    """
    server = f"{workspace_name}-ondemand.sql.azuresynapse.net"
    database = "master"

    jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;\
        trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

    token = get_az_cli_token("https://database.windows.net/.default")

    connection_properties = {"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "accessToken": token}

    return jdbc_url, connection_properties


def sync_synapse_tables_to_local_spark(
    workspace_name: str,
    object_sync_details: List[ObjectSyncDetails],
    overwrite: bool = False,
    spark: SparkSession = None,
):
    """Syncs tables from a Synapse workspace to a local Spark metastore.

    Args:
        workspace_name (str): Name of the Synapse workspace.
        table_infos (List[ObjectSyncDetails]): List of ObjectSyncDetails containing info about the tables to sync.
        overwrite (bool, optional): Whether to overwrite the tables if they already exist in the local metastore.
            Defaults to False, to avoid unnecessary pulling of data from Synapse (and therefore egress charges).
        spark (SparkSession, optional): The Spark session to use. Defaults to a new local Spark session with default
            values for metastore location and warehouse directory.
    """
    existing_spark_session = spark

    if not existing_spark_session:
        spark = get_or_create_spark_session(LocalSparkSessionConfig("sync_synapse_tables_to_local_spark"))

    jdbc_url, connection_properties = _get_jdbc_connection_properties(workspace_name)

    for osd in object_sync_details:
        _ = spark.sql(f"CREATE DATABASE IF NOT EXISTS {osd.database_name}")

        for table in osd.tables:
            table_exists = spark.catalog.tableExists(table, osd.database_name)

            if table_exists and not overwrite:
                print(
                    "\033[93m"
                    + f"Table '{table}' in database '{osd.database_name}' already exists and \
overwrite is set to False. Skipping table sync."
                    + "\033[0m"
                )
                continue
            else:
                spark.read.jdbc(
                    url=jdbc_url, table=f"{osd.database_name}.dbo.{table}", properties=connection_properties
                ).coalesce(1).write.format("delta").mode("overwrite").saveAsTable(f"{osd.database_name}.{table}")

    if not existing_spark_session:
        spark.stop()
