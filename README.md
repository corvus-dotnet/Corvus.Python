# Corvus.Python

This provides a library of Python utility functions and classes, generally in the data and analytics space. Many components have been designed to help streamline local development of cloud-based solutions.

## Sub-modules

### `spark_utils`

| Component Name                    | Object Type | Description                                                                                                                                                                                                                 | Import syntax                                                                 |
|-----------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| <code>get_spark_utils</code>      | Function    | Returns spark utility functions corresponding to current environment (local/Synapse) based on mssparkutils API. Useful for local development. <b>Note:</b> Config file required for local development - see [section below](#configuration). | <code>from corvus_python.spark_utils import get_spark_utils</code>      |


#### `get_spark_utils()`

##### Supported operations

The currently supported operations of the [`mssparkutils` API](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python) are as follows:

- `credentials`
    - `getSecretWithLS(linkedService, secret)`
    - `getToken(audience)`
- `env`
    - `getWorkspaceName()`

##### Configuration

This function requires a configuration file to be present in the repo, and for the file to follow a particular structure. Namely, the config file has been designed to largely mimic the API interface of the [`mssparkutils` API](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python).

There is a top-level property for each (supported) sub-module of `mssparkutils`. Second-level properties follow the names of the functions associated with each sub-module. Within these second-level properties, the config structure depends on the implementation of the mirrored function found in the corresponding class in the package. E.g. the structure of `credentials.getSecretWithLS()` can be inferred from the [`LocalCredentialUtils` class](https://github.com/corvus-dotnet/Corvus.Python/blob/main/src/corvus_python/pyspark/utilities/spark_utils/local_spark_utils.py#L34-L56).

Below shows the current, complete specification of the config file for the supported operations (NOTE: not all operations require configuration):

```
{
    "credentials": {
        "getSecretWithLS": {
            "<linked_service_name>": {
                "<key_vault_secret_name>": {
                    "type": "static",
                    "value": "<key_vault_secret_value>"
                }
            }
        },
        "getToken": {
            "tenantId": "<tenant_id (optional)>"
        }
    },
    "env": {
        "getWorkspaceName": "<workspace_name>"
    }
}
```

By default, a file in the root of the current working directory with file name `local-spark-utils-config.json` will be automatically discovered. If the file resides in a different location, and/or has a different file name, then the absolute path must be specified when calling `get_spark_utils()`.


### `pyspark.utilities`

**⚠️ Note: This module requires the 'pyspark' extra to be installed: `corvus-python[pyspark]`**

Includes utility functions when working with PySpark to build data processing solutions. Primary API interfaces:

| Component Name                    | Object Type | Description                                                                                                                                                                                                                 | Import syntax                                                                 |
|-----------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| <code>get_or_create_spark_session</code> | Function    | Gets or creates a Spark Session, depending on the environment. Supports Synapse or a Local Spark Session configuration.                                                                                                      | <code>from corvus_python.pyspark.utilities import get_or_create_spark_session</code> |
| <code>null_safe_join</code>       | Function    | Joins two Spark DataFrames incorporating null-safe equality.                                                                                                                                                                | <code>from corvus_python.pyspark.utilities import null_safe_join</code>       |
---

### `pyspark.synapse`

**⚠️ Note: This module requires the 'pyspark' extra to be installed: `corvus-python[pyspark]`**

| Component Name                                  | Object Type | Description                                                                                                                                                               | Import syntax                                                                     |
|-------------------------------------------------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| <code>sync_synapse_tables_to_local_spark</code> | Function    | Reads tables from a Synapse SQL Serverless endpoint and clones to a local Hive metastore. Useful for local development, to avoid continuously sending data over the wire. | <code>from corvus_python.pyspark.synapse import sync_synapse_tables_to_local_spark</code> |
| <code>ObjectSyncDetails</code>                  | Class       | Dataclass representing a database and corresponding tables to be synced using the <code>sync_synapse_tables_to_local_spark</code> function.                               | <code>from corvus_python.pyspark.synapse import ObjectSyncDetails</code>                  |

#### `sync_synapse_tables_to_local_spark()`

Here is an example code snippet to utilize this function:

```python
from corvus_python.pyspark.synapse import sync_synapse_tables_to_local_spark, ObjectSyncDetails

sync_synapse_tables_to_local_spark(
    workspace_name='my_workspace_name',
    object_sync_details=[
        ObjectSyncDetails(
            database_name='database_1',
            tables=['table_1', 'table_2']
        ),
        ObjectSyncDetails(
            database_name='database_2',
            tables=['table_1', 'table_2']
        )
    ],
    # overwrite = True,  # Uncomment if local clones already exist and you wish to overwrite.
    # spark = spark,     # Uncomment if you wish to provide your own Spark Session (assumed stored within "spark" variable).
)
```

---

### `synapse`

Includes utility functions when working with Synapse Analytics. Primary API interfaces:

| Component Name                | Object Type | Description                                                                                                              | Import syntax                                                |
|-------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>SynapseUtilities</code> | Class | A utility class for interacting with Azure Synapse Analytics. | <code>from corvus_python.synapse import SynapseUtilities</code> |

---

### `auth`

Includes utility functions when working with authentication libraries within Python. Primary API interfaces:

| Component Name                | Object Type | Description                                                                                                              | Import syntax                                                |
|-------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>get_az_cli_token</code> | Function    | Gets an Entra ID token from the Azure CLI for a specified resource (/audience) and tenant. Useful for local development. | <code>from corvus_python.auth import get_az_cli_token</code> |
|                               |             |                                                                                                                          |                                                              |

### `sharepoint`

Includes utility functions when working with SharePoint REST API. Primary API interfaces:

| Component Name                        | Object Type | Description                                                                                           | Import syntax                                                |
|---------------------------------------|-------------|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>SharePointUtilities</code>      | Class       | A utility class for interacting with SharePoint REST API.                                             | <code>from corvus_python.sharepoint import SharePointUtilities</code> |

---

### `email`

Includes utility classes and models for sending emails using Azure Communication Services (ACS). Primary API interfaces:

| Component Name                | Object Type | Description                                                                                           | Import syntax                                                |
|-------------------------------|-------------|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>AcsEmailService</code>  | Class       | A service class for sending emails through Azure Communication Services.                              | <code>from corvus_python.email import AcsEmailService</code> |
| <code>EmailContent</code>     | Class       | Dataclass representing email content including subject, plain text, and HTML.                         | <code>from corvus_python.email import EmailContent</code>    |
| <code>EmailRecipients</code>  | Class       | Dataclass representing email recipients (to, cc, bcc).                                               | <code>from corvus_python.email import EmailRecipients</code> |
| <code>EmailRecipient</code>   | Class       | Dataclass representing a single email recipient with address and display name.                        | <code>from corvus_python.email import EmailRecipient</code>  |
| <code>EmailAttachment</code>  | Class       | Dataclass representing an email attachment with name, content type, and base64-encoded content.       | <code>from corvus_python.email import EmailAttachment</code> |
| <code>EmailError</code>       | Class       | Exception class for email-related errors.                                                            | <code>from corvus_python.email import EmailError</code>      |

#### Usage Example

```python
from corvus_python.email import (
    AcsEmailService, 
    EmailContent, 
    EmailRecipients, 
    EmailRecipient, 
    EmailAttachment
)

# Initialize the email service
email_service = AcsEmailService(
    acs_connection_string="your_acs_connection_string",
    from_email="sender@yourdomain.com",
    email_sending_disabled=False  # Set to True for testing/development
)

# Create email content
content = EmailContent(
    subject="Welcome to Our Service",
    plain_text="Welcome! Thank you for joining our service.",
    html="<h1>Welcome!</h1><p>Thank you for joining our service.</p>"
)

# Define recipients
recipients = EmailRecipients(
    to=[
        EmailRecipient("user1@example.com", "User One"),
        EmailRecipient("user2@example.com", "User Two")
    ],
    cc=[EmailRecipient("manager@example.com", "Manager")],
    bcc=[EmailRecipient("admin@example.com", "Admin")]
)

# Optional: Add attachments
attachments = [
    EmailAttachment(
        name="welcome_guide.pdf",
        content_type="application/pdf",
        content_in_base64="base64_encoded_content_here"
    )
]

# Send the email
try:
    email_service.send_email(content, recipients, attachments)
    print("Email sent successfully!")
except EmailError as e:
    print(f"Failed to send email: {e}")
```

**Configuration Notes:**
- Requires an Azure Communication Services resource with email capability configured
- The `from_email` must use a configured MailFrom address from your ACS resource
- Set `email_sending_disabled=True` during development to prevent actual emails from being sent
- Attachments must be base64-encoded before adding to the `EmailAttachment` object

---

### `storage`

Provides storage configuration abstractions for data lake operations, with implementations for local and Azure Data Lake Gen2 storage.

| Component Name | Object Type | Description | Import syntax |
|---|---|---|---|
| <code>DataLakeLayer</code> | Enum | Enumeration of data lake layers: `BRONZE`, `SILVER`, `GOLD`. | <code>from corvus_python.storage import DataLakeLayer</code> |
| <code>StorageConfiguration</code> | Class (abstract) | Base class for storage configurations. Provides a `get_full_path()` method and `storage_options` dict. | <code>from corvus_python.storage import StorageConfiguration</code> |
| <code>LocalFileSystemStorageConfiguration</code> | Class | Storage configuration backed by the local file system. Useful for local development. | <code>from corvus_python.storage import LocalFileSystemStorageConfiguration</code> |
| <code>AzureDataLakeFileSystemPerLayerConfiguration</code> | Class | ADLS Gen2 configuration where each data lake layer maps to a separate file system (`bronze`, `silver`, `gold`). | <code>from corvus_python.storage import AzureDataLakeFileSystemPerLayerConfiguration</code> |
| <code>AzureDataLakeSingleFileSystemConfiguration</code> | Class | ADLS Gen2 configuration using a single file system with top-level folders for each layer. | <code>from corvus_python.storage import AzureDataLakeSingleFileSystemConfiguration</code> |

#### Usage Example

```python
from corvus_python.storage import (
    DataLakeLayer,
    LocalFileSystemStorageConfiguration,
    AzureDataLakeFileSystemPerLayerConfiguration,
    AzureDataLakeSingleFileSystemConfiguration,
)

# Local filesystem (for development)
local_config = LocalFileSystemStorageConfiguration(base_path="./data")
path = local_config.get_full_path(DataLakeLayer.BRONZE, "my_database/my_table")
# -> ./data/bronze/my_database/my_table

# Azure Data Lake - separate file system per layer
adls_per_layer = AzureDataLakeFileSystemPerLayerConfiguration(
    storage_account_name="mystorageaccount",
    storage_options={"account_key": "..."},
)
path = adls_per_layer.get_full_path(DataLakeLayer.SILVER, "my_database/my_table")
# -> abfss://silver@mystorageaccount.dfs.core.windows.net/my_database/my_table

# Azure Data Lake - single file system
adls_single = AzureDataLakeSingleFileSystemConfiguration(
    storage_account_name="mystorageaccount",
    file_system_name="datalake",
)
path = adls_single.get_full_path(DataLakeLayer.GOLD, "my_database/my_table")
# -> abfss://datalake@mystorageaccount.dfs.core.windows.net/gold/my_database/my_table
```

---

### `repositories`

Provides repository classes for reading and writing structured data across various storage backends, built on [Polars](https://pola.rs/). All repositories accept a `StorageConfiguration` to abstract over local and cloud storage.

#### Supporting Data Classes

| Component Name | Object Type | Description | Import syntax |
|---|---|---|---|
| <code>DatabaseDefinition</code> | Dataclass | Defines a logical database by name and a list of `TableDefinition` instances. | <code>from corvus_python.repositories import DatabaseDefinition</code> |
| <code>TableDefinition</code> | Dataclass | Defines a table by name, Pandera schema, optional title, and optional `db_schema`. | <code>from corvus_python.repositories import TableDefinition</code> |

#### `PolarsDeltaTableRepository`

Manages Delta Lake tables within a specified data lake layer. Handles schema validation using Pandera and integrates with OpenTelemetry for tracing.

```python
from corvus_python.repositories import PolarsDeltaTableRepository, DatabaseDefinition, TableDefinition
from corvus_python.storage import LocalFileSystemStorageConfiguration, DataLakeLayer
```

| Method | Description |
|---|---|
| `read_data(table_name)` | Reads a Delta table into a `LazyFrame`. Returns `None` if the table is empty. |
| `overwrite_table(table_name, data, overwrite_schema=False)` | Overwrites the table after eagerly validating the full dataset against its Pandera schema. |
| `overwrite_table_lazy(table_name, data, overwrite_schema=False)` | Overwrites the table using streaming execution. Performs schema-level validation only. |
| `overwrite_table_with_condition(table_name, data, predicate, overwrite_schema=False)` | Overwrites only rows matching the given predicate (e.g. for partition-level updates). |
| `append_to_table(table_name, data)` | Appends data to an existing Delta table. |

#### `PolarsCsvDataRepository`

Reads CSV files from a hive-partitioned path (`snapshot_time=<timestamp>/<name>.csv`).

```python
from corvus_python.repositories import PolarsCsvDataRepository
```

| Method | Description |
|---|---|
| `load_csv(object_name, snapshot_timestamp, include_file_paths=None)` | Loads a CSV file into a `DataFrame`. Strips `.csv` suffix from `object_name` if present. |

#### `PolarsExcelDataRepository`

Reads Excel workbooks from a hive-partitioned path, returning all sheets as a `dict[str, DataFrame]`.

```python
from corvus_python.repositories import PolarsExcelDataRepository
```

| Method | Description |
|---|---|
| `load_excel(snapshot_timestamp, workbook_name, relative_path=None)` | Loads all sheets from an `.xlsx` workbook into a dict keyed by sheet name. |

#### `PolarsNdJsonDataRepository`

Reads Newline Delimited JSON (NDJSON) files from a partitioned path (`<load_type>/snapshot_time=<timestamp>/<name>.json`).

```python
from corvus_python.repositories import PolarsNdJsonDataRepository
```

| Method | Description |
|---|---|
| `load_ndjson(object_name, load_type, snapshot_timestamp, include_file_paths=None, schema_overrides=None, schema=None)` | Loads an NDJSON file into a `DataFrame`. Supports schema overrides and full schema specification. |

#### `PolarsAzureTableRepository`

Queries Azure Table Storage, returning results as Polars DataFrames. Authenticates using `DefaultAzureCredential`.

```python
from corvus_python.repositories import PolarsAzureTableRepository
```

| Method | Description |
|---|---|
| `query(table_name, query_filter, parameters, schema=None)` | Queries an Azure Table with an OData filter and named parameters. |
| `get_entities_partition_key_starts_with(table_name, partition_key_prefix, schema=None)` | Retrieves all entities whose `PartitionKey` starts with the given prefix. |
