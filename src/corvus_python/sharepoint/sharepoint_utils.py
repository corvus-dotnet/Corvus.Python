import requests
from urllib.parse import urlparse
import base64
from pathlib import Path


class SharePointUtilities():
    @staticmethod
    def retrieve_image_as_base64(
        sharepoint_url: str,
        token: str,
    ) -> str:
        """Retrieves an image from SharePoint and returns it as its base64 representation.

        Args:
            sharepoint_url (str): URL of the image in SharePoint.
            token (str): Bearer token for the request.

        Returns:
            str: base64 representation of the SharePoint image.
        """
        if not sharepoint_url:
            return None

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }

        parsed_url = urlparse(sharepoint_url)
        path_segments = parsed_url.path.split('/')[1:]

        sharepoint_tenant_fqdn = parsed_url.netloc
        sharepoint_site_name = path_segments[1]
        library_name = path_segments[2]
        file_path = '/'.join(path_segments[3:])
        file_ext = Path(file_path).suffix

        drive_id = SharePointUtilities._get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        # Get file by relative path (/drives/{drive-id}/root:/{file_path})
        get_file_request_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}"
        get_file_response = requests.get(get_file_request_url, headers=headers)

        if get_file_response.status_code == 404:
            alternate_file_ext = ".png" if file_ext == ".jpg" else ".jpg"
            print(
                f"Unsuccessfully attempted to retrieve file with URL {get_file_request_url} "
                f"with file extension {file_ext}. Attempting retrieval as {alternate_file_ext}."
            )

            alternate_file_path = file_path.replace(file_ext, alternate_file_ext)
            get_file_request_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{alternate_file_path}"
            get_file_response = requests.get(get_file_request_url, headers=headers)
            get_file_response.raise_for_status()

        download_url = get_file_response.json()["@microsoft.graph.downloadUrl"]

        # Download the image
        download_response = requests.get(download_url)
        download_response.raise_for_status()  # Raise an exception if the request failed

        # Convert the image data to a base64 string
        image_data = download_response.content
        base64_image = base64.b64encode(image_data).decode("utf-8")

        return base64_image

    @staticmethod
    def save_file(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        file_path: str,
        token: str,
        file_bytes: bytearray
    ) -> str:
        """Saves a file to SharePoint and returns the web URL.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant to save the file to.
            sharepoint_site_name (str): Name of the SharePoint site to save the file to.
            library_name (str): Name of the library to save the file to (URL-encoded).
            file_path (str): Full file path (relative to root folder) to use when saving. Don't start with slash.
            token (str): Bearer token for the request.
            file_bytes (bytearray): Content of the file.

        Returns:
            dict: File response as JSON.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }

        drive_id = SharePointUtilities._get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        # Upload file
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/root:/{file_path}:/content"

        response = requests.put(url, headers=headers, data=file_bytes)
        response.raise_for_status()

        return response.json()

    @staticmethod
    def update_sharepoint_columns(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        item_id: str,
        column_updates: dict,
        token: str,
    ) -> None:
        """Updates SharePoint metadata columns associated with a file.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant to save the file to.
            sharepoint_site_name (str): Name of the SharePoint site to save the file to.
            library_name (str): Name of the library to save the file to (URL-encoded).
            item_id (str): Item ID of the file.
            column_updates (dict): Dictionary of updates to the SharePoint columns {"<col_name>": <new_col_value>}.
            token (str): Bearer token for the request.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }

        drive_id = SharePointUtilities._get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        # Update col values
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields"

        response = requests.patch(url, headers=headers, json=column_updates)
        response.raise_for_status()

    @staticmethod
    def get_file_download_url(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        file_name: str,
        token: str
    ) -> str:
        """Returns the pre-authed download URL for a file in SharePoint.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant where the file lives.
            sharepoint_site_name (str): Name of the SharePoint site where the file lives.
            library_name (str): Name of the library where the file lives.
            file_name (str): File name of the file.
            token (str): Bearer token for the request.

        Returns:
            str: Web URL to download the file.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }

        drive_id = SharePointUtilities._get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        # Get file download URL
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}"
            "?select=@microsoft.graph.downloadUrl"
        )

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()["@microsoft.graph.downloadUrl"]

    @staticmethod
    def get_download_urls_for_files_in_folder(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        folder_name: str,
        token: str
    ) -> str:
        """Returns the list of files in a folder with pre-authed download URLs.

        Args:
            sharepoint_tenant_fqdn (str): FQDN of the SharePoint tenant where the folder lives.
            sharepoint_site_name (str): Name of the SharePoint site where the folder lives.
            library_name (str): Name of the library where the folder lives.
            file_name (str): Name of the folder.
            token (str): Bearer token for the request.

        Returns:
            str: Web URL to download the file.
        """

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}"
        }

        drive_id = SharePointUtilities._get_drive_id(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        # Get the data response
        url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_name}:/children"
            "?$select=id,name,content.downloadUrl,file"
        )

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()["value"]

    @staticmethod
    def _get_drive_id(
        sharepoint_tenant_fqdn: str,
        sharepoint_site_name: str,
        library_name: str,
        headers: str
    ) -> str:
        web_url = f"https://{sharepoint_tenant_fqdn}/sites/{sharepoint_site_name}/{library_name}"

        # Get drives for site
        list_drives_request_url = (
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives"
        )
        list_drives_response = requests.get(list_drives_request_url, headers=headers)
        list_drives_response.raise_for_status()  # Raise an exception if the request failed

        # Find drive that matches on web URL
        drive_id = next((d["id"] for d in list_drives_response.json()["value"] if d["webUrl"] == web_url))

        return drive_id
