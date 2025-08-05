"""Copyright (c) Endjin Limited. All rights reserved."""

from dataclasses import dataclass, field
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
from corvus_python.storage import StorageConfiguration, LocalFileSystemStorageConfiguration


CWD = os.path.join(os.getcwd())


@dataclass
class LocalSparkSessionConfig():
    """Class to represent configuration of a Local Spark Session.

    Attributes:
        workload_name (str): The name of the workload. This will be used as the name of the Spark application.
        storage_configuration (StorageConfiguration, optional): The storage configuration to use for the Spark
            session. Defaults to a LocalFileSystemStorageConfiguration with the data directory set to the current
            working directory.
        warehouse_dir (str, optional): The directory in which to provision the default Spark SQL Warehouse. Defaults to
            the current working directory.
        enable_hive_support (bool, optional): Whether to enable Hive support. Defaults to True. When set to true,
            the persistent hive metastore will be created in the current working directory.
        hive_metastore_dir (str, optional): The directory in which to provision the Hive metastore. Defaults to the
            current working directory.
        install_hadoop_azure_package (bool, optional): Whether to install the hadoop-azure package. Defaults to False.
            Should be set to True if using Azure Data Lake Storage Gen 2.
        enable_az_cli_auth (bool, optional): Whether to enable Azure CLI authentication. Defaults to False. If using
            Azure Data Lake Storage Gen 2, this should be set to True to enable authentication using your current
            Azure CLI credentials.
        enable_spark_ui (bool, optional): Whether to enable the Spark UI. Defaults to True. It can be useful to turn
            the UI off for unit tests to avoid port binding issues.
        additional_spark_config (dict, optional): Additional Spark configuration to set. Defaults to an empty dict.
    """
    workload_name: str
    storage_configuration: StorageConfiguration = LocalFileSystemStorageConfiguration(os.path.join(CWD, "data"))
    warehouse_dir: str = os.path.join(CWD, "warehouse")
    enable_hive_support: bool = True
    hive_metastore_dir: str = os.path.join(CWD, "metastore")
    install_hadoop_azure_package: bool = False
    enable_az_cli_auth: bool = False
    enable_spark_ui: bool = True
    extra_packages: list = field(default_factory=lambda: [])
    additional_spark_config: dict = field(default_factory=lambda: {})


@dataclass
class LocalSparkSession():
    """Class to represent a Local Spark session.
    Attributes:
        config (LocalSparkSessionConfig): The configuration for the local Spark session.
    """
    config: LocalSparkSessionConfig

    def create_spark_session(self) -> SparkSession:
        """Creates a Local Spark session with Delta Lake support, using the config provided by the
            LocalSparkSessionConfig. This is intended to be used for local development and testing.

        Returns:
            SparkSession: The Spark session.
        """
        builder = (
            SparkSession.builder.appName(self.config.workload_name)
            .master("local[*]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config(
                "spark.ui.enabled",
                "true" if self.config.enable_spark_ui else "false")
        )

        extra_packages = self.config.extra_packages

        builder = builder.config("spark.sql.warehouse.dir", self.config.warehouse_dir)
        extra_packages.append("com.microsoft.sqlserver:mssql-jdbc:12.6.0.jre11")

        if self.config.install_hadoop_azure_package:
            extra_packages.append("org.apache.hadoop:hadoop-azure:3.3.3")

        if self.config.enable_az_cli_auth:
            builder = builder.config(
                "spark.jars.repositories", "https://pkgs.dev.azure.com/endjin-labs/hadoop/_packaging/hadoop/maven/v1")
            extra_packages.append("com.endjin.hadoop:hadoop-azure-token-providers:1.0.1")

        if self.config.enable_hive_support:
            # Modify where the hive metastore and derby log files are stored. This is necessary because the default
            # location is in the system's temporary directory, which is not guaranteed to be the same across different
            # runs of the app. This can cause issues with the metastore not being found when the app is restarted.
            hive_metastore_dir = self.config.hive_metastore_dir

            builder = builder \
                .config(
                    "javax.jdo.option.ConnectionURL",
                    f"jdbc:derby:;databaseName={hive_metastore_dir}/metastore_db;create=true"
                ) \
                .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={hive_metastore_dir}") \
                .enableHiveSupport()

        if self.config.additional_spark_config:
            for k, v in self.config.additional_spark_config.items():
                builder = builder.config(k, v)

        spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

        if self.config.enable_az_cli_auth:
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.azure.account.auth.type",
                "Custom"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.azure.account.oauth.provider.type",
                "com.endjin.hadoop.fs.azurebfs.custom.AzureCliCredentialTokenProvider"
            )

        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("default_database", self.config.workload_name)

        return spark
