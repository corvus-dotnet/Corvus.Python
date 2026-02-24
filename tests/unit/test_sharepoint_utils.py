import base64
import pytest
import requests
from unittest.mock import patch, Mock
from corvus_python.sharepoint.sharepoint_utils import SharePointUtilities

# region Tests for retrieve_image_as_base64


def test_retrieve_image_as_base64_success():
    drive_id = "fake_drive_id"
    file_path = "folder/file.jpg"
    token = "fake_token"
    mock_download_url = "https://example.com/download/file.jpg"
    mock_image_data = b"fake_image_data"
    expected_base64_image = base64.b64encode(mock_image_data).decode("utf-8")

    with patch("requests.get") as mock_get:
        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}),
            Mock(status_code=200, content=mock_image_data),
        ]

        result = SharePointUtilities.retrieve_image_as_base64(drive_id, file_path, token)

        # Verify the first call to get file metadata
        mock_get.assert_any_call(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )

        assert result == expected_base64_image


def test_retrieve_image_as_base64_file_not_found():
    drive_id = "fake_drive_id"
    file_path = "folder/file.jpg"
    token = "fake_token"
    mock_download_url = "https://example.com/download/file.png"
    mock_image_data = b"fake_image_data"
    expected_base64_image = base64.b64encode(mock_image_data).decode("utf-8")

    with patch("requests.get") as mock_get:
        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=404),  # First attempt fails
            Mock(
                status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}
            ),  # Second attempt succeeds
            Mock(status_code=200, content=mock_image_data),
        ]

        result = SharePointUtilities.retrieve_image_as_base64(drive_id, file_path, token)
        assert result == expected_base64_image


def test_retrieve_image_as_base64_download_failure():
    drive_id = "fake_drive_id"
    file_path = "folder/file.jpg"
    token = "fake_token"
    mock_download_url = "https://example.com/download/file.jpg"

    with patch("requests.get") as mock_get:
        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}),
            Mock(status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())),
        ]

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.retrieve_image_as_base64(drive_id, file_path, token)


# endregion

# region Tests for save_file


def test_save_file_success():
    drive_id = "fake_drive_id"
    file_path = "folder/file.txt"
    token = "fake_token"
    file_bytes = bytearray(b"fake_file_content")
    mock_response_json = {
        "id": "file_id",
        "webUrl": "https://example.sharepoint.com/sites/site_name/library_name/folder/file.txt",
    }

    with patch("requests.put") as mock_put:
        # Mock the response for uploading the file
        mock_put.return_value = Mock(status_code=200, json=lambda: mock_response_json)

        result = SharePointUtilities.save_file(drive_id, file_path, token, file_bytes)

        mock_put.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/root:/{file_path}:/content",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
            data=file_bytes,
        )

        assert result == mock_response_json


def test_save_file_failure():
    drive_id = "fake_drive_id"
    file_path = "folder/file.txt"
    token = "fake_token"
    file_bytes = bytearray(b"fake_file_content")

    with patch("requests.put") as mock_put:
        # Mock the response for uploading the file
        mock_put.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.save_file(drive_id, file_path, token, file_bytes)

        mock_put.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/root:/{file_path}:/content",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
            data=file_bytes,
        )


# endregion

# region Tests for update_sharepoint_columns


def test_update_sharepoint_columns_success():
    drive_id = "fake_drive_id"
    item_id = "item_id"
    column_updates = {"Title": "New Title"}
    token = "fake_token"

    with patch("requests.patch") as mock_patch:
        # Mock the response for updating the columns
        mock_patch.return_value = Mock(status_code=200)

        SharePointUtilities.update_sharepoint_columns(drive_id, item_id, column_updates, token)

        mock_patch.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
            json=column_updates,
        )


def test_update_sharepoint_columns_failure():
    drive_id = "fake_drive_id"
    item_id = "item_id"
    column_updates = {"Title": "New Title"}
    token = "fake_token"

    with patch("requests.patch") as mock_patch:
        # Mock the response for updating the columns
        mock_patch.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.update_sharepoint_columns(drive_id, item_id, column_updates, token)

        mock_patch.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/listItem/fields",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
            json=column_updates,
        )


# endregion

# region Tests for get_file_download_url


def test_get_file_download_url_success():
    drive_id = "fake_drive_id"
    file_name = "file.txt"
    token = "fake_token"
    mock_download_url = "https://example.com/download/file.txt"

    with patch("requests.get") as mock_get:
        # Mock the response for getting the file download URL
        mock_get.return_value = Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url})

        result = SharePointUtilities.get_file_download_url(drive_id, file_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}"
                "?select=@microsoft.graph.downloadUrl"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )

        assert result == mock_download_url


def test_get_file_download_url_failure():
    drive_id = "fake_drive_id"
    file_name = "file.txt"
    token = "fake_token"

    with patch("requests.get") as mock_get:
        # Mock the response for getting the file download URL
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_file_download_url(drive_id, file_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_name}"
                "?select=@microsoft.graph.downloadUrl"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )


# endregion

# region Tests for get_download_urls_for_files_in_folder


def test_get_download_urls_for_files_in_folder_success():
    drive_id = "fake_drive_id"
    folder_name = "folder_name"
    token = "fake_token"
    mock_files = [
        {"id": "file1", "name": "file1.txt", "content.downloadUrl": "https://example.com/download/file1.txt"},
        {"id": "file2", "name": "file2.txt", "content.downloadUrl": "https://example.com/download/file2.txt"},
    ]

    with patch("requests.get") as mock_get:
        # Mock the response for getting the files in the folder
        mock_get.return_value = Mock(status_code=200, json=lambda: {"value": mock_files})

        result = SharePointUtilities.get_download_urls_for_files_in_folder(drive_id, folder_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_name}:/children"
                "?$select=id,name,content.downloadUrl,file"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )

        assert result == mock_files


def test_get_download_urls_for_files_in_folder_failure():
    drive_id = "fake_drive_id"
    folder_name = "folder_name"
    token = "fake_token"

    with patch("requests.get") as mock_get:
        # Mock the response for getting the files in the folder
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_download_urls_for_files_in_folder(drive_id, folder_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_name}:/children"
                "?$select=id,name,content.downloadUrl,file"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )


# endregion

# region Tests for get_items_in_folder


def test_get_items_in_folder_success():
    drive_id = "fake_drive_id"
    folder_name = "folder_name"
    token = "fake_token"
    mock_folders = [
        {
            "id": "folder1",
            "name": "folder1",
            "webUrl": "https://example.sharepoint.com/sites/site_name/library_name/folder_name/folder1",
            "folder": {},
        },
        {
            "id": "folder2",
            "name": "folder2",
            "webUrl": "https://example.sharepoint.com/sites/site_name/library_name/folder_name/folder2",
            "folder": {},
        },
    ]

    with patch("requests.get") as mock_get:
        # Mock the response for getting the folders in the folder
        mock_get.return_value = Mock(status_code=200, json=lambda: {"value": mock_folders})

        result = SharePointUtilities.get_items_in_folder(drive_id, folder_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_name}:/children"
                "?$select=id,name,webUrl,file,folder"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )

        assert result == mock_folders


def test_get_items_in_folder_failure():
    drive_id = "fake_drive_id"
    folder_name = "folder_name"
    token = "fake_token"

    with patch("requests.get") as mock_get:
        # Mock the response for getting the folders in the folder
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_items_in_folder(drive_id, folder_name, token)

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{drive_id}/"
                f"root:/{folder_name}:/children"
                "?$select=id,name,webUrl,file,folder"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )


# endregion

# region Tests for get_drive_id


def test_get_drive_id_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    token = "fake_token"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {token}",
    }
    mock_drive_id = "fake_drive_id"
    mock_web_url = f"https://{sharepoint_tenant_fqdn}/sites/{sharepoint_site_name}/{library_name}"
    mock_drives_response = {
        "value": [
            {"id": "other_drive_id", "webUrl": "https://example.sharepoint.com/sites/other_site/other_library"},
            {"id": mock_drive_id, "webUrl": mock_web_url},
        ]
    }

    with patch("requests.get") as mock_get:
        # Mock the response for listing the drives
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_drives_response)

        result = SharePointUtilities.get_drive_id(sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        mock_get.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives",
            headers=headers,
        )

        assert result == mock_drive_id


def test_get_drive_id_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": "Bearer fake_token",
    }

    with patch("requests.get") as mock_get:
        # Mock the response for listing the drives
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError())
        )

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_drive_id(sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        mock_get.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives",
            headers=headers,
        )


# endregion
