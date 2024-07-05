from corvus_python.pyspark.utilities import create_spark_session, apply_schema_to_dataframe, select_in_target_order
from corvus_python.pyspark.storage import LocalFileSystemStorageConfiguration
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType


def test_dataframe_select():
    spark = create_spark_session(
        "test_df_Select",
        LocalFileSystemStorageConfiguration("./data"),
        enable_hive_support=False
    )

    test_schema_1 = StructType([
        StructField("id", IntegerType(), False, {"source_col": "identifier"}),
        StructField("name", StructType(
            [
                StructField("first", StringType(), False, {"source_col": "first"}),
                StructField("last", StringType(), False, {"source_col": "last"}),
                StructField("previous_ages", ArrayType(IntegerType()), False, {"source_col": "previous_names"})
            ]),
            False
        ),
        StructField("age", IntegerType(), False),
        StructField("complex", ArrayType(StructType(
            [
                StructField("something", LongType(), True)
            ])),
            False
        ),
    ])

    test_schema_2 = StructType([
        StructField("id", IntegerType(), False, {"source_col": "identifier"}),
        StructField("name", StructType(
            [
                StructField("previous_ages", ArrayType(IntegerType()), False, {"source_col": "previous_names"}),
                StructField("last", StringType(), False, {"source_col": "last"}),
                StructField("first", StringType(), False, {"source_col": "first"}),
            ]),
            False
        ),
        StructField("complex", ArrayType(StructType(
            [
                StructField("something_else", IntegerType(), True),
                StructField("something", IntegerType(), True),
            ])),
            False
        ),
        StructField("age", IntegerType(), False),
    ])

    test_data = [
        (1, ("John", "Doe", [1, 2, 3]), 21, [{"something": 1, "something_else": 3}, {"something": 2, "something_else": 4}]),
        (2, ("Jane", "Doe", [4, 5, 6]), 22, []),
        (3, ("Jim", "Doe", [7]), 23, [])
    ]

    df = spark.createDataFrame(test_data, test_schema_1)

    df = select_in_target_order(spark.createDataFrame(test_data, test_schema_1), test_schema_2)

    assert df
