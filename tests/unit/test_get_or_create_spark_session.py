from corvus_python.pyspark.utilities import LocalSparkSessionConfig, get_or_create_spark_session


def test_get_or_create_spark_session():
    # Arrange
    local_config = LocalSparkSessionConfig("dummy")

    # Act
    spark = get_or_create_spark_session(local_config)

    # Assert
    assert spark is not None
