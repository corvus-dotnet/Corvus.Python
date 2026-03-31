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

### `sql`

Includes utility functions for working with SQL databases via pyodbc, with AAD token-based authentication. Provides helpers for connecting to Synapse serverless SQL and Fabric SQL Analytics endpoints, managing views over Delta Lake tables, and executing DDL statements.

**⚠️ Note: This module requires ODBC Driver 18 for SQL Server or later.**

| Component Name                                               | Object Type | Description                                                                                                          | Import syntax                                                                                          |
|--------------------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| <code>get_pyodbc_connection</code>                           | Function    | Opens a pyodbc connection to a Synapse serverless SQL or Fabric SQL Analytics endpoint using AAD tokens.             | <code>from corvus_python.sql import get_pyodbc_connection</code>                                       |
| <code>get_pyodbc_connection_with_token</code>                | Function    | Opens a pyodbc connection using a pre-acquired AAD token. Useful when running inside Synapse notebooks.              | <code>from corvus_python.sql import get_pyodbc_connection_with_token</code>                            |
| <code>execute_ddl</code>                                     | Function    | Executes a DDL statement (e.g. CREATE VIEW) using a pyodbc connection.                                               | <code>from corvus_python.sql import execute_ddl</code>                                                 |
| <code>create_or_alter_view_over_delta_table</code>           | Function    | Creates or alters a SQL view over a Delta Lake table, with support for inferred or explicit column types. | <code>from corvus_python.sql import create_or_alter_view_over_delta_table</code>                       |
| <code>drop_views_in_schema</code>                            | Function    | Drops all views in a given schema.                                                                                   | <code>from corvus_python.sql import drop_views_in_schema</code>                                        |
| <code>SelectColumn</code>                                    | Class       | Dataclass representing a column to select, with an optional display title.                                           | <code>from corvus_python.sql import SelectColumn</code>                                                |
| <code>WithColumn</code>                                      | Class       | Dataclass representing a column with an explicit type for use in OPENROWSET WITH clauses.                            | <code>from corvus_python.sql import WithColumn</code>                                                  |

#### `sql.synapse`

Convenience wrappers specific to Synapse serverless SQL endpoints.

| Component Name                                               | Object Type | Description                                                                                                          | Import syntax                                                                                          |
|--------------------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| <code>get_synapse_sql_pyodbc_connection</code>               | Function    | Opens a pyodbc connection to a Synapse serverless SQL endpoint using AAD tokens. Builds the server URL from the workspace name. | <code>from corvus_python.sql.synapse import get_synapse_sql_pyodbc_connection</code>           |
| <code>get_synapse_sql_pyodbc_connection_with_token</code>    | Function    | Opens a pyodbc connection to a Synapse serverless SQL endpoint using a pre-acquired AAD token. Ideal for Synapse notebooks. | <code>from corvus_python.sql.synapse import get_synapse_sql_pyodbc_connection_with_token</code> |
| <code>create_database_if_not_exists</code>                   | Function    | Creates a database if it doesn't already exist. Requires a connection to the master database.                        | <code>from corvus_python.sql.synapse import create_database_if_not_exists</code>                       |

#### `sql.fabric`

Convenience wrappers specific to Fabric SQL Analytics endpoints.

| Component Name                                               | Object Type | Description                                                                                                          | Import syntax                                                                                          |
|--------------------------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| <code>get_fabric_sql_pyodbc_connection</code>                | Function    | Opens a pyodbc connection to a Fabric SQL Analytics endpoint using AAD tokens.                                       | <code>from corvus_python.sql.fabric import get_fabric_sql_pyodbc_connection</code>                     |

#### Usage Example

```python
from corvus_python.sql import (
    get_pyodbc_connection,
    create_or_alter_view_over_delta_table,
    drop_views_in_schema,
    SelectColumn,
    WithColumn,
)

# Connect using DefaultAzureCredential
conn = get_pyodbc_connection(
    server="myworkspace-ondemand.sql.azuresynapse.net",
    database="my_database",
    use_managed_identity=False,
)

# Create a view with inferred types
create_or_alter_view_over_delta_table(
    conn=conn,
    schema_name="dbo",
    view_name="my_view",
    delta_table_path="abfss://container@storageaccount.dfs.core.windows.net/path/to/table",
    infer_types=True,
    select_columns=[
        SelectColumn(name="id"),
        SelectColumn(name="full_name", title="Name"),
    ],
)

# Create a view with explicit types
create_or_alter_view_over_delta_table(
    conn=conn,
    schema_name="dbo",
    view_name="my_typed_view",
    delta_table_path="abfss://container@storageaccount.dfs.core.windows.net/path/to/table",
    infer_types=False,
    with_columns=[
        WithColumn(name="id", type="INT"),
        WithColumn(name="full_name", type="VARCHAR(200)", title="Name"),
    ],
)

# Drop all views in a schema
drop_views_in_schema(conn, schema_name="dbo")
```

Or use the Synapse/Fabric-specific helpers:

```python
from corvus_python.sql.synapse import get_synapse_sql_pyodbc_connection

conn = get_synapse_sql_pyodbc_connection(
    workspace_name="myworkspace",
    database="my_database",
    use_managed_identity=False,
)
```

#### Usage from a Synapse Notebook

When running inside a Synapse notebook, you can use `mssparkutils` to acquire a token and pass it directly:

```python
from corvus_python.sql.synapse import get_synapse_sql_pyodbc_connection_with_token

token = mssparkutils.credentials.getToken("DW")

conn = get_synapse_sql_pyodbc_connection_with_token(
    workspace_name="myworkspace",
    database="my_database",
    token=token,
)
```
