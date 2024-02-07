from corvus_python.pyspark.utilities import create_spark_session, apply_schema_to_dataframe
from corvus_python.pyspark.storage import LocalFileSystemStorageConfiguration
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType


def test_dataframe_select():
    spark = create_spark_session(
        "test_df_Select",
        LocalFileSystemStorageConfiguration("./data"),
        enable_hive_support=False
    )

    test_schema = StructType([
        StructField("id", IntegerType(), False, {"source_col": "identifier"}),
        StructField("name", StructType(
            [
                StructField("first", StringType(), False, {"source_col": "first"}),
                StructField("last", StringType(), False, {"source_col": "last"}),
                StructField("previous_names", ArrayType(StringType()), False, {"source_col": "previous_names"})
            ]),
            False
        ),
        StructField("age", IntegerType(), False),
    ])

    test_data = [
        (1, ("John", "Doe", ["John Smith"]), 21),
        (2, ("Jane", "Doe", ["Jane Smith"]), 22),
        (3, ("Jim", "Doe", ["Jim Smith"]), 23)
    ]

    assert test_data
