# Corvus.Python

This provides a library of Python utility functions and classes, generally in the data and analytics space. Many components have been designed to help streamline local development of cloud-based solutions.

## Sub-modules

### `pyspark.utilities`

**⚠️ Note: This module requires the 'pyspark' extra to be installed: `corvus-python[pyspark]`**

Includes utility functions when working with PySpark to build data processing solutions. Primary API interfaces:

| Component Name                    | Object Type | Description                                                                                                                                                                                                                 | Import syntax                                                                 |
|-----------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| <code>get_or_create_spark_session</code> | Function    | Gets or creates a Spark Session, depending on the environment. Supports Synapse or a Local Spark Session configuration.                                                                                                      | <code>from corvus_python.pyspark.utilities import get_or_create_spark_session</code> |
| <code>get_spark_utils</code>      | Function    | Returns spark utility functions corresponding to current environment (local/Synapase) based on mssparkutils API. Useful for local development. <b>Note:</b> Config file required for local development - see [section below](#configuration). | <code>from corvus_python.pyspark.utilities import get_spark_utils</code>      |
| <code>null_safe_join</code>       | Function    | Joins two Spark DataFrames incorporating null-safe equality.                                                                                                                                                                | <code>from corvus_python.pyspark.utilities import null_safe_join</code>       |
|                                   |             |                                                                                                                                                                                                                             |                                                                               |

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

### Auth

Includes utility functions when working with authentication libraries within Python. Primary API interfaces:

| Component Name                | Object Type | Description                                                                                                              | Import syntax                                                |
|-------------------------------|-------------|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>get_az_cli_token</code> | Function    | Gets an Entra ID token from the Azure CLI for a specified resource (/audience) and tenant. Useful for local development. | <code>from corvus_python.auth import get_az_cli_token</code> |
|                               |             |                                                                                                                          |                                                              |

### SharePoint

Includes utility functions when working with SharePoint REST API. Primary API interfaces:

| Component Name                        | Object Type | Description                                                                                           | Import syntax                                                |
|---------------------------------------|-------------|-------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| <code>SharePointUtilities</code>      | Class       | A utility class for interacting with SharePoint REST API.                                             | <code>from corvus_python.sharepoint import SharePointUtilities</code> |
