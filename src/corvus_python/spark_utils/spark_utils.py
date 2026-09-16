"""Copyright (c) Endjin Limited. All rights reserved."""

import os
import json
from corvus_python.spark_utils.local_spark_utils import LocalSparkUtils
from corvus_python.platform import FABRIC, SYNAPSE, get_platform


def get_spark_utils(
    local_spark_utils_config_file_path: str = f"{os.getcwd()}/local-spark-utils-config.json",
):
    """Returns spark utility functions corresponding to the current environment.

    Args:
        local_spark_utils_config_file_path (str): Path to the config used to instantiate the `LocalSparkUtils` class.
            Defaults to a file located in the root of the current working directory.

    Returns:
        object: An instance of the spark utility functions.

    Raises:
        NotImplementedError: If called on Fabric, which has no mssparkutils equivalent.
        FileNotFoundError: If the local-spark-utils-config.json file is not found at the specified path.
    """
    platform = get_platform()

    if platform == FABRIC:
        # Fabric has no linked services and its workspace is not a Synapse workspace, so there is
        # nothing faithful to return. Failing here beats falling through to the local branch and
        # reporting a missing local-spark-utils-config.json in a Fabric notebook.
        raise NotImplementedError(
            "get_spark_utils has no Fabric implementation. Use corvus_python.fabric "
            "(FabricTokenCredential, get_variable_library_value) with the Azure SDKs instead."
        )

    if platform == SYNAPSE:
        from notebookutils import mssparkutils

        return mssparkutils

    try:
        with open(local_spark_utils_config_file_path) as f:
            config = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"""
Could not find local-spark-utils-config.json at {local_spark_utils_config_file_path}.
Please ensure a config file is at this location or pass in an absolute path to the file if it is located elsewhere.
Please see `https://github.com/corvus-dotnet/Corvus.Python` for more information.
                """
        )

    return LocalSparkUtils(config)
