"""Copyright (c) Endjin Limited. All rights reserved."""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
from ..storage import StorageConfiguration, LocalFileSystemStorageConfiguration


def create_spark_session(
        workload_name: str,
        file_system_configuration: StorageConfiguration,
        enable_hive_support: bool = True,
        install_hadoop_azure_package: bool = False,
        enable_az_cli_auth: bool = False) -> SparkSession:
    """Creates a Spark session with Delta Lake support. This is intended to be used for local development and testing.

    Args:
        workload_name (str): The name of the workload. This will be used as the name of the Spark application.
        file_system_configuration (StorageConfiguration): The storage configuration to use for the Spark session.
        enable_hive_support (bool, optional): Whether to enable Hive support. Defaults to True. When set to true,
            the persistent hive metastore will be created in the current working directory.
        install_hadoop_azure_package (bool, optional): Whether to install the hadoop-azure package. Defaults to False.
            Should be set to True if using Azure Data Lake Storage Gen 2.
        enable_az_cli_auth (bool, optional): Whether to enable Azure CLI authentication. Defaults to False. If using
            Azure Data Lake Storage Gen 2, this should be set to True to enable authentication using your current
            Azure CLI credentials.
    """

    builder = (
        SparkSession.builder.appName(workload_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    extra_packages = []

    if isinstance(file_system_configuration, LocalFileSystemStorageConfiguration):
        # Modify where databases are stored by default (i.e. when not specifying the `LOCATION` clause in a `CREATE
        # DATABASE` statement).
        builder = builder.config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "warehouse"))
        extra_packages.append("com.microsoft.sqlserver:mssql-jdbc:12.6.0.jre11")

    if install_hadoop_azure_package:
        extra_packages.append("org.apache.hadoop:hadoop-azure:3.3.3")

    if enable_az_cli_auth:
        builder = builder.config(
            "spark.jars.repositories", "https://pkgs.dev.azure.com/endjin-labs/hadoop/_packaging/hadoop/maven/v1")
        extra_packages.append("com.endjin.hadoop:hadoop-azure-token-providers:1.0.1")

    if enable_hive_support:
        # Modify where the hive metastore and derby log files are stored. This is necessary because the default location
        # is in the system's temporary directory, which is not guaranteed to be the same across different runs of the
        # app. This can cause issues with the metastore not being found when the app is restarted.
        hive_metastore_dir = os.path.join(os.getcwd(), "metastore")

        builder = builder \
            .config(
                "javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={hive_metastore_dir}/metastore_db;create=true"
            ) \
            .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={hive_metastore_dir}") \
            .enableHiveSupport()

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

    if enable_az_cli_auth:
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.azure.account.auth.type",
            "Custom"
        )
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.azure.account.oauth.provider.type",
            "com.endjin.hadoop.fs.azurebfs.custom.AzureCliCredentialTokenProvider"
        )

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("default_database", workload_name)

    return spark
