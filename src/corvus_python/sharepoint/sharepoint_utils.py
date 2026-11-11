from typing import Any
import requests
from urllib.parse import urlparse
import base64
from pathlib import Path

from ..http_timeouts import DEFAULT_HTTP_TIMEOUT, DEFAULT_UPLOAD_HTTP_TIMEOUT, HttpTimeout


class SharePointUtilities:
    """Static helpers for the SharePoint parts of the Microsoft Graph API.

    Every HTTP request made by these helpers has a timeout, which each method takes as an optional keyword-only
    ``timeout`` argument: either a number of seconds or a ``(connect, read)`` tuple. It defaults to
    ``corvus_python.http_timeouts.DEFAULT_HTTP_TIMEOUT``, or ``DEFAULT_UPLOAD_HTTP_TIMEOUT`` for ``save_file``. A
    request that times out raises ``requests.Timeout``, which is not caught or retried.
    """

    @staticmethod
    def retrieve_image_as_base64(
        drive_id: str,
        file_path: str,
        token: str,
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> str:
        """Retrieves an image from SharePoint and returns it as its base64 representation.

        Args:
            drive_id (str): ID of the SharePoint drive containing the image.
            file_path (str): Relative path to the image file within the drive.
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for each HTTP request (the file
                lookups and the download), as one value or a (connect, read) tuple. Defaults to
                DEFAULT_HTTP_TIMEOUT.

        Returns:
            str: base64 representation of the SharePoint image.

        Raises:
            requests.Timeout: If any of the requests times out.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        file_ext = Path(file_path).suffix

        # Get file by relative path (/drives/{drive-id}/root:/{file_path})
        get_file_request_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}"
        get_file_response = requests.get(get_file_request_url, headers=headers, timeout=timeout)

        if get_file_response.status_code == 404:
            alternate_file_ext = ".png" if file_ext == ".jpg" else ".jpg"
            print(
                f"Unsuccessfully attempted to retrieve file with URL {get_file_request_url} "
                f"with file extension {file_ext}. Attempting retrieval as {alternate_file_ext}."
            )

            alternate_file_path = file_path.replace(file_ext, alternate_file_ext)
            get_file_request_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{alternate_file_path}"
            get_file_response = requests.get(get_file_request_url, headers=headers, timeout=timeout)
            get_file_response.raise_for_status()

        download_url = get_file_response.json()["@microsoft.graph.downloadUrl"]

        # Download the image
        download_response = requests.get(download_url, timeout=timeout)
        download_response.raise_for_status()  # Raise an exception if the request failed

        # Convert the image data to a base64 string
        image_data = download_response.content
        base64_image = base64.b64encode(image_data).decode("utf-8")

        return base64_image

    @staticmethod
    def assign_item_permissions(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        item_path: str,
        token: str,
        recipients: list[dict[str, Any]],
        write_permission: bool = False,
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> dict[str, Any]:
        """Assigns permissions on an item in SharePoint.
        See https://learn.microsoft.com/en-us/graph/api/driveitem-invite.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant to save the file to.
            sharepoint_site_name (str): Name of the SharePoint site to save the file to.
            library_name (str): Name of the library to save the file to (URL-encoded).
            item_path (str): Full item path (relative to root folder) to use when saving. Don't start with slash.
            token (str): Bearer token for the request.
            recipients: Array of driveRecipient objects.
            See https://learn.microsoft.com/en-us/graph/api/resources/driverecipient
            write_permission (bool): Boolean indicating whether to assign write permission
            timeout (float | tuple[float, float], optional): Timeout in seconds for each HTTP request (the drive
                lookup and the invite), as one value or a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            dict: Response as JSON.

        Raises:
            requests.Timeout: If any of the requests times out.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        roles = ["write"] if write_permission else ["read"]
        body = {"requireSignIn": True, "sendInvitation": False, "roles": roles, "recipients": recipients}
        drive_id = SharePointUtilities.get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers, timeout=timeout
        )
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{item_path}:/invite"
        response = requests.post(url, headers=headers, json=body, timeout=timeout)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def rename_file(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        existing_file_path: str,
        token: str,
        new_file_name: str,
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        """Renames a file in SharePoint.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant to save the file to.
            sharepoint_site_name (str): Name of the SharePoint site to save the file to.
            library_name (str): Name of the library to save the file to (URL-encoded).
            existing_file_path (str): Full file path (relative to root folder) of the file to rename.
            Don't start with slash.
            token (str): Bearer token for the request.
            new_file_name (str): The new file name (including extension).
            timeout (float | tuple[float, float], optional): Timeout in seconds for each HTTP request (the drive
                lookup and the rename), as one value or a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Raises:
            requests.Timeout: If any of the requests times out.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        body = {"name": new_file_name}
        drive_id = SharePointUtilities.get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers, timeout=timeout
        )
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/root:/{existing_file_path}"
        response = requests.patch(url, headers=headers, json=body, timeout=timeout)
        response.raise_for_status()

    @staticmethod
    def get_sharepoint_path_segments(sharepoint_url: str):
        """
        Parses a SharePoint URL and extracts the tenant FQDN, site name, and library name.

        Args:
            sharepoint_url (str): The full URL to a SharePoint resource.

        Returns:
            tuple: A tuple containing:
                - sharepoint_tenant_fqdn (str): The fully qualified domain name of the SharePoint tenant.
                - sharepoint_site_name (str): The name of the SharePoint site.
                - library_name (str): The name of the document library.
        """
        parsed_url = urlparse(sharepoint_url)
        path_segments = parsed_url.path.split("/")[1:]

        sharepoint_tenant_fqdn = parsed_url.netloc
        sharepoint_site_name = path_segments[1]
        library_name = path_segments[2]
        return sharepoint_tenant_fqdn, sharepoint_site_name, library_name

    @staticmethod
    def save_file(
        drive_id: str,
        file_path: str,
        token: str,
        file_bytes: bytearray,
        *,
        timeout: HttpTimeout = DEFAULT_UPLOAD_HTTP_TIMEOUT,
    ) -> str:
        """Saves a file to SharePoint and returns the file response as JSON.

        Args:
            drive_id (str): ID of the SharePoint drive to save the file to.
            file_path (str): Full file path (relative to root folder) to use when saving. Don't start with slash.
            token (str): Bearer token for the request.
            file_bytes (bytearray): Content of the file.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the upload request, as one value
                or a (connect, read) tuple. Defaults to DEFAULT_UPLOAD_HTTP_TIMEOUT, whose read timeout is longer than
                DEFAULT_HTTP_TIMEOUT's to allow for large uploads.

        Returns:
            dict: File response as JSON.

        Raises:
            requests.Timeout: If the request times out.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        # Upload file
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/root:/{file_path}:/content"

        response = requests.put(url, headers=headers, data=file_bytes, timeout=timeout)
        response.raise_for_status()

        return response.json()

    @staticmethod
    def update_sharepoint_columns(
        drive_id: str,
        item_id: str,
        column_updates: dict[str, Any],
        token: str,
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        """Updates SharePoint metadata columns associated with a file.

        Args:
            drive_id (str): ID of the SharePoint drive containing the file.
            item_id (str): Item ID of the file.
            column_updates (dict): Dictionary of updates to the SharePoint columns {"<col_name>": <new_col_value>}.
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the HTTP request, as one value or
                a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Raises:
            requests.Timeout: If the request times out.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        # Update col values
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields"

        response = requests.patch(url, headers=headers, json=column_updates, timeout=timeout)
        response.raise_for_status()

    @staticmethod
    def get_file_download_url(
        drive_id: str, file_name: str, token: str, *, timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT
    ) -> str:
        """Returns the pre-authed download URL for a file in SharePoint.

        Args:
            drive_id (str): ID of the SharePoint drive where the file lives.
            file_name (str): File name of the file.
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the HTTP request, as one value or
                a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            str: Pre-authenticated download URL for the file.

        Raises:
            requests.Timeout: If the request times out.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        # Get file download URL
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}"
            "?select=@microsoft.graph.downloadUrl"
        )

        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()

        return response.json()["@microsoft.graph.downloadUrl"]

    @staticmethod
    def get_download_urls_for_files_in_folder(
        drive_id: str, folder_name: str, token: str, *, timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT
    ) -> str:
        """Returns the list of files in a folder with pre-authed download URLs.

        Args:
            drive_id (str): ID of the SharePoint drive where the folder lives.
            folder_name (str): Name of the folder.
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the HTTP request, as one value or
                a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            list: List of files in the folder with their download URLs and metadata.

        Raises:
            requests.Timeout: If the request times out.
        """

        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        # Get the data response
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_name}:/children"
            "?$select=id,name,content.downloadUrl,file"
        )

        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()

        return response.json()["value"]

    @staticmethod
    def get_items_in_folder(
        drive_id: str, folder_path: str, token: str, *, timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT
    ) -> list[str]:
        """
        Returns all items within a specified folder in a SharePoint drive.

        Args:
            drive_id (str): ID of the SharePoint drive.
            folder_path (str): Path to the parent folder (relative to root, do not start with slash).
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the HTTP request, as one value or
                a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            list: List of items in the folder.

        Raises:
            requests.Timeout: If the request times out.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
            "?$select=id,name,webUrl,file,folder"
        )
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        # Only return items that are folders (have a 'folder' property)
        items = response.json()["value"]
        folders = [item for item in items if "folder" in item]
        return folders

    @staticmethod
    def get_drive_id(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        headers: dict[str, str],
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> str:
        """Gets the drive ID for a SharePoint document library.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant. Takes the form of "<tenant>.sharepoint.com".
            sharepoint_site_name (str): Name of the SharePoint site.
            library_name (str): Name of the document library.
            headers (dict[str, str]): HTTP headers including authorization token.
            timeout (float | tuple[float, float], optional): Timeout in seconds for the HTTP request, as one value or
                a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            str: Drive ID of the specified document library.

        Raises:
            requests.Timeout: If the request times out.
        """
        web_url = f"https://{sharepoint_tenant_fqdn}/sites/{sharepoint_site_name}/{library_name}"

        # Get drives for site
        list_drives_request_url = (
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives"
        )
        list_drives_response = requests.get(list_drives_request_url, headers=headers, timeout=timeout)
        list_drives_response.raise_for_status()  # Raise an exception if the request failed

        # Find drive that matches on web URL
        drive_id = next((d["id"] for d in list_drives_response.json()["value"] if d["webUrl"] == web_url))

        return drive_id

    @staticmethod
    def get_list_items(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        list_name: str,
        token: str,
        *,
        timeout: HttpTimeout = DEFAULT_HTTP_TIMEOUT,
    ) -> list[dict[str, Any]]:
        """Returns all items from a SharePoint list.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant. Takes the form of "<tenant>.sharepoint.com".
            sharepoint_site_name (str): Name of the SharePoint site.
            list_name (str): Name or ID of the SharePoint list.
            token (str): Bearer token for the request.
            timeout (float | tuple[float, float], optional): Timeout in seconds for each HTTP request (one per page
                of results), as one value or a (connect, read) tuple. Defaults to DEFAULT_HTTP_TIMEOUT.

        Returns:
            list[dict[str, Any]]: List of items from the SharePoint list, each containing their field values.

        Raises:
            requests.Timeout: If any of the requests times out.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        }

        site_id = f"{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}"
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}:/lists/{list_name}/items?$expand=fields"

        items: list[dict[str, Any]] = []

        while url:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()

            data = response.json()
            items.extend(data["value"])
            url = data.get("@odata.nextLink")

        return items
