from unittest.mock import Mock, patch

import pytest

from corvus_python.http_timeouts import DEFAULT_HTTP_TIMEOUT
from corvus_python.synapse import SynapseUtilities


def _call_every_request_method(synapse: SynapseUtilities) -> None:
    synapse.create_pipeline_run("pipeline", {})
    synapse.wait_for_pipeline_run("run_id")
    synapse.get_workspace_resource_id()
    synapse.get_linked_service("linked_service")


@pytest.mark.parametrize(
    "constructor_kwargs, expected_timeout",
    [({}, DEFAULT_HTTP_TIMEOUT), ({"timeout": (1, 2)}, (1, 2))],
    ids=["default", "caller_supplied"],
)
def test_every_request_passes_timeout(constructor_kwargs, expected_timeout):
    response = Mock(status_code=200, json=lambda: {"runId": "run_id", "status": "Succeeded", "id": "resource_id"})
    synapse = SynapseUtilities("workspace", "token", **constructor_kwargs)

    with patch("requests.get", return_value=response) as mock_get, patch(
        "requests.post", return_value=response
    ) as mock_post:
        _call_every_request_method(synapse)

    calls = mock_post.call_args_list + mock_get.call_args_list
    assert len(calls) == 4
    assert all(c.kwargs["timeout"] == expected_timeout for c in calls)
