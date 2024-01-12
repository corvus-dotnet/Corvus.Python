import spark_startup  # noqa F401
from delta import DeltaTable
from pyspark.sql import SparkSession # noqa E402

spark = SparkSession.getActiveSession()
db_name = spark.conf.get("default_database")


def test_sample():
    assert True
