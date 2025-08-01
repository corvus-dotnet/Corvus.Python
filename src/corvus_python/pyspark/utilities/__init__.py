# flake8: noqa
from .spark_utils.spark_utils import get_spark_utils

# Only import pyspark-dependent modules if pyspark is available
from .._optional_imports import check_pyspark_available

if check_pyspark_available():
    from .dataframe_utils.null_safe_join import null_safe_join
    from .session_utils.local_spark_session import LocalSparkSessionConfig, LocalSparkSession
    from .session_utils.get_or_create_spark_session import get_or_create_spark_session
else:
    # Provide stub functions that raise informative errors
    def null_safe_join(*args, **kwargs):
        raise ImportError("PySpark is required for null_safe_join functionality. "
                          "Install with: pip install corvus-python[pyspark]")
    
    def get_or_create_spark_session(*args, **kwargs):
        raise ImportError("PySpark is required for get_or_create_spark_session functionality. "
                          "Install with: pip install corvus-python[pyspark]")
    
    class LocalSparkSessionConfig:
        def __init__(self, *args, **kwargs):
            raise ImportError("PySpark is required for LocalSparkSessionConfig functionality. "
                              "Install with: pip install corvus-python[pyspark]")
    
    class LocalSparkSession:
        def __init__(self, *args, **kwargs):
            raise ImportError("PySpark is required for LocalSparkSession functionality. "
                              "Install with: pip install corvus-python[pyspark]")