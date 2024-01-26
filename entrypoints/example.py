from corvus_python.pyspark.utilities import get_spark_utils

spark_utils = get_spark_utils()

cred = spark_utils.credentials.getSecretWithLS("KeyVault", "EnvironmentName")

print(cred)

token = spark_utils.credentials.getToken("Synapse")

print(token)

workspace_name = spark_utils.env.getWorkspaceName()

print(workspace_name)
