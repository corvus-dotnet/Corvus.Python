"""Copyright (c) Endjin Limited. All rights reserved."""

import os
import json
from corvus_python.spark_utils.local_spark_utils import LocalSparkUtils
from corvus_python.platform import FABRIC, SYNAPSE, get_platform


def get_spark_utils(
    local_spark_utils_config_file_path: str = f"{os.getcwd()}/local-spark-utils-config.json",
):
    """Returns the notebook utilities for the current environment.

    - Synapse: `notebookutils.mssparkutils`.
    - Fabric: Fabric's native `notebookutils`, which works in both Spark and Python notebooks.
    - Local: `LocalSparkUtils`, a partial mirror of the mssparkutils API driven by a config file.

    The Fabric and Synapse objects are not interchangeable. Fabric has no linked services, so
    `credentials.getSecretWithLS` does not exist, and `credentials.getToken` needs full resource
    scopes rather than Synapse's aliases. Use `corvus_python.platform.get_platform()` where behaviour
    needs to differ.

    Args:
        local_spark_utils_config_file_path (str): Path to the config used to instantiate the `LocalSparkUtils` class.
            Defaults to a file located in the root of the current working directory.

    Returns:
        object: The notebook utilities for the current environment.

    Raises:
        FileNotFoundError: If running locally and the local-spark-utils-config.json file is not found at the
            specified path.
    """
    platform = get_platform()

    if platform == FABRIC:
        # The flattened namespace rather than mssparkutils, which is only a compatibility alias on
        # Fabric and is not proven in Python notebooks.
        import notebookutils

        return notebookutils

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
