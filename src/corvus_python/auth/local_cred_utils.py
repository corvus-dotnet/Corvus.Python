from azure.identity import AzureCliCredential, CredentialUnavailableError
from azure.core.exceptions import ClientAuthenticationError


def get_az_cli_token(resource: str, tenant_id: str = None) -> str:
    """Gets the Azure CLI token for a resource.

    Args:
        resource (str): The resource to get the token for.
        tenant_id (str): The tenant ID to use for authentication.

    Returns:
        AzureCliCredential: The Azure CLI credential for the resource.

    Raises:
        RuntimeError: If user is not logged into the Azure CLI, or logged into the wrong tenant.
    """
    credential = AzureCliCredential()

    try:
        token = credential.get_token(resource, tenant_id=tenant_id).token
    except CredentialUnavailableError:
        raise RuntimeError("Please login to the Azure CLI using `az login --tenant <tenant> --use-device-code` "
                           "to authenticate.")
    except ClientAuthenticationError:
        raise RuntimeError("It is likely that you're logged into the wrong tenant. "
                           "Please login to the Azure CLI using `az login --tenant <tenant> --use-device-code` "
                           "to authenticate.")

    return token
