from behave import given, when, then
from corvus_python.pyspark.utilities import LocalSparkSessionConfig, get_or_create_spark_session
import os


@given('I use the default LocalSparkSessionConfig configuration')
def step_use_default_config(context):
    context.local_config = LocalSparkSessionConfig("dummy")


@when('I create a Spark Session')
def step_create_spark_session(context):
    # Create a Spark session with the given name
    context.spark = get_or_create_spark_session(context.local_config)


@then('the Spark Session should be created and valid')
def step_check_spark_session_created(context):
    # Check if the Spark session was created
    assert context.spark is not None


@given('I create a Spark Session with the default LocalSparkSessionConfig configuration with the name "{session_name}"')
def step_create_local_spark_session(context, session_name):
    context.spark = get_or_create_spark_session(LocalSparkSessionConfig(session_name))


@when('I write an empty DataFrame to a Delta table named "{table_name}"')
def step_write_empty_dataframe(context, table_name):
    context.table_name = table_name
    df = context.spark.createDataFrame([], "id INT, value STRING")
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)


@then('the table should exist in the default database')
def step_check_table_exists(context):
    tables = context.spark.catalog.listTables()
    assert context.table_name in [t.name for t in tables], f"Table {context.table_name} does not exist in the default database."


@then('the table path should exist')
def step_check_table_path_exists(context):
    table_path = f"warehouse/{context.table_name}"
    assert os.path.exists(table_path), f"The path for table {context.table_name} does not exist."
