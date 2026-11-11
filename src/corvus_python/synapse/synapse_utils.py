import requests
import urllib.parse
from datetime import datetime, timedelta
import time

from corvus_python.http_timeouts import DEFAULT_HTTP_TIMEOUT, HttpTimeout
from corvus_python.monitoring.tracing import all_methods_start_new_current_span_with_method_name
from opentelemetry import trace

tracer = trace.get_tracer(__name__)


class SynapsePipelineError(Exception):
    """
    Exception raised for errors that occur during the execution of a Synapse pipeline.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        super().__init__(message)


class SynapsePipelineStatus:
    """
    Represents the possible status values for a Synapse pipeline.
    """

    QUEUED: str = "Queued"
    IN_PROGRESS: str = "InProgress"
    SUCCEEDED: str = "Succeeded"
    FAILED: str = "Failed"
    CANCELING: str = "Canceling"
    CANCELLED: str = "Cancelled"


@all_methods_start_new_current_span_with_method_name(tracer)
class SynapseUtilities:
    """
    A utility class for interacting with Azure Synapse Analytics.

    This class provides methods for creating and monitoring pipeline runs,
    retrieving workspace information, and fetching linked services.

    Every HTTP request it makes has a timeout, set by the optional keyword-only ``timeout`` argument: either a number
    of seconds or a ``(connect, read)`` tuple. It defaults to ``corvus_python.http_timeouts.DEFAULT_HTTP_TIMEOUT``.
    A request that times out raises ``requests.Timeout``, which is not caught or retried.
    """

    def __init__(self, workspace_name: str, access_token: str, *, timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT):
        self.workspace_endpoint = f"https://{workspace_name}.dev.azuresynapse.net"
        self.access_token = access_token
        self.timeout = timeout

    def create_pipeline_run(
        self,
        pipeline_name: str,
        pipeline_parameters: dict[str, str],
    ) -> str:
        """
        Creates a new pipeline run in Azure Synapse Analytics.

        Args:
            pipeline_name (str): The name of the pipeline.
            pipeline_parameters (dict[str, str]): The parameters for the pipeline.

        Returns:
            str: The ID of the created pipeline run.

        Raises:
            requests.Timeout: If the request times out.
        """

        url = (
            f"{self.workspace_endpoint}/pipelines/"
            f"{urllib.parse.quote(pipeline_name)}/createRun?api-version=2020-12-01"
        )

        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.post(url, json=pipeline_parameters, headers=headers, timeout=self.timeout)

        pipeline_run_id = response.json()["runId"]

        return pipeline_run_id

    def wait_for_pipeline_run(self, run_id: str, timeout_mins: int = 60) -> str:
        """
        Waits for a pipeline run to complete in Azure Synapse Analytics.

        Args:
            run_id (str): The ID of the pipeline run to wait for.
            timeout_mins (int, optional): The maximum number of minutes to wait for the pipeline run to complete.
                Defaults to 60.

        Returns:
            str: The final status of the pipeline run.

        Raises:
            requests.Timeout: If a status request times out. This is separate from ``timeout_mins``, which limits
                the total time spent waiting for the run.
        """

        url = f"{self.workspace_endpoint}/pipelineruns/{run_id}?api-version=2020-12-01"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        completed = False
        status = None
        now = datetime.utcnow()
        timeout_at = now + timedelta(minutes=timeout_mins)

        while not completed:
            response = requests.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 200:
                status = response.json()["status"]

                if status in [
                    SynapsePipelineStatus.SUCCEEDED,
                    SynapsePipelineStatus.FAILED,
                    SynapsePipelineStatus.CANCELLED,
                ]:
                    print(f"Status for pipeline run '{run_id}': {status}.")
                    completed = True
                else:
                    print(f"Status for pipeline run '{run_id}': {status}. Waiting for 10 seconds...")
                    time.sleep(10)
                    now = datetime.utcnow()
                    if now > timeout_at:
                        raise SynapsePipelineError(f"Timed out waiting for pipeline run '{run_id}' to complete.")

            else:
                raise SynapsePipelineError(f"Error when polling for pipeline run '{run_id}' status: {response.json()}")

        return status

    def get_pipeline_run_portal_url(self, pipeline_run_id: str):
        """
        Gets the portal URL for a pipeline run in Azure Synapse Analytics.

        Args:
            pipeline_run_id (str): The ID of the pipeline run.

        Returns:
            str: The URL of the pipeline run in the Azure Synapse Analytics portal.

        Raises:
            requests.Timeout: If the workspace request times out.
        """
        workspace_resource_id = self.get_workspace_resource_id()
        workspace_resource_id_encoded = urllib.parse.quote_plus(workspace_resource_id)

        return (
            f"https://web.azuresynapse.net/en/monitoring/pipelineruns/{pipeline_run_id}"
            f"?workspace={workspace_resource_id_encoded}"
        )

    def get_workspace_resource_id(self) -> str:
        """
        Retrieves the resource ID of the Azure Synapse Analytics workspace.

        Returns:
            str: The resource ID of the workspace.

        Raises:
            requests.Timeout: If the request times out.
        """
        url = f"{self.workspace_endpoint}/workspace?api-version=2020-12-01"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(url, headers=headers, timeout=self.timeout)

        resource_id = response.json()["id"]

        return resource_id

    def get_linked_service(self, linked_service_name: str) -> str:
        """
        Retrieves the details of a linked service in Azure Synapse Analytics.

        Args:
            linked_service_name (str): The name of the linked service.

        Returns:
            str: The JSON representation of the linked service.

        Raises:
            requests.Timeout: If the request times out.
        """
        url = f"{self.workspace_endpoint}/linkedservices/{linked_service_name}?api-version=2020-12-01"

        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(url, headers=headers, timeout=self.timeout)

        return response.json()
