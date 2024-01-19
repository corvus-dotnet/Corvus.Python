"""Copyright (c) Endjin Limited. All rights reserved."""

import os
import json
from notebookutils import mssparkutils
from .local_spark_utils import LocalSparkUtils


def get_spark_utils(local_spark_utils_config_file_path: str = f"{os.getcwd()}/local-spark-utils-config.json"):
    if os.environ.get("MMLSPARK_PLATFORM_INFO") == "synapse":
        return mssparkutils
    else:
        try:
            with open(local_spark_utils_config_file_path) as f:
                config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"""

Could not find local-spark-utils-config.json at {local_spark_utils_config_file_path}.

Please ensure a config file is at this location or pass in an absolute path to the file if it is located elsewhere.

Please see https://github.com/corvus-dotnet/Corvus.Python for more information.
                """)

        return LocalSparkUtils(config)
