import logging
from typing import Dict
import polars as pl
from azure.data.tables import TableServiceClient, EntityProperty
from azure.identity import DefaultAzureCredential


class PolarsAzureTableRepository:
    """
    Repository for interacting with Azure Table Storage using Polars DataFrames.
    """

    def __init__(self, storage_account_name: str):
        """
        Initializes the repository with the given storage account name.

        Args:
            storage_account_name (str): The name of the Azure storage account.
        """
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.table_service_client = TableServiceClient(
            endpoint=f"https://{storage_account_name}.table.core.windows.net",
            credential=DefaultAzureCredential(),
        )

    def query(
        self, table_name: str, query_filter: str, parameters: Dict[str, str], schema: dict[str, pl.DataType] = None
    ) -> pl.DataFrame:
        """
        Queries data from the specified Azure Table and loads it into a Polars DataFrame.

        Args:
            table_name (str): The name of the Azure Table to load data from.
            query_filter (str): The query to filter the data.
            parameters (Dict[str, str]): Parameters for the query filter.
            schema (dict[str, pl.DataType]): Optional schema for the resulting DataFrame.
        Returns:
            pl.DataFrame: The data loaded from the Azure Table as a Polars DataFrame.
        """
        self.logger.info("query_table - Table name: %s - Query: %s", table_name, query_filter)

        table_client = self.table_service_client.get_table_client(table_name)
        entities = list(table_client.query_entities(query_filter, parameters=parameters))

        if not entities:
            self.logger.warning("query_table - No data found in table: %s", table_name)
            return pl.DataFrame(schema=schema)

        # Some types have their values wrapped in an EntityProperty (GUID, INT64, BINARY)
        for entity in entities:
            for key, value in list(entity.items()):
                if isinstance(value, EntityProperty):
                    entity[key] = value.value

        df = pl.DataFrame(entities, schema=schema)
        self.logger.info("query_table - Loaded %d records from table: %s", df.height, table_name)
        return df

    def get_entities_partition_key_starts_with(
        self, table_name: str, partition_key_prefix: str, schema: dict[str, pl.DataType] = None
    ) -> pl.DataFrame:
        """
        Retrieves entities from the specified Azure Table where the PartitionKey starts with the given prefix.

        Args:
            table_name (str): The name of the Azure Table to query.
            partition_key_prefix (str): The prefix to filter PartitionKeys.
            schema (dict[str, pl.DataType]): Optional schema for the resulting DataFrame.

        Returns:
            pl.DataFrame: The data loaded from the Azure Table as a Polars DataFrame.
        """
        query_filter = "PartitionKey ge @prefix and PartitionKey lt @next_prefix"
        next_prefix = partition_key_prefix[:-1] + chr(ord(partition_key_prefix[-1]) + 1)

        parameters = {
            "prefix": partition_key_prefix,
            "next_prefix": next_prefix,
        }

        return self.query(table_name, query_filter, parameters, schema=schema)
