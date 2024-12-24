import base64
import pytest
import requests
from unittest.mock import patch, Mock
from corvus_python.sharepoint.sharepoint_utils import SharePointUtilities

# region Tests for retrieve_image_as_base64


def test_retrieve_image_as_base64_success():
    sharepoint_url = "https://example.sharepoint.com/sites/site_name/library_name/file.jpg"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"
    mock_download_url = "https://example.com/download/file.jpg"
    mock_image_data = b"fake_image_data"
    expected_base64_image = base64.b64encode(mock_image_data).decode("utf-8")

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}),
            Mock(status_code=200, content=mock_image_data)
        ]

        result = SharePointUtilities.retrieve_image_as_base64(sharepoint_url, token)
        SharePointUtilities._get_drive_id.assert_called_once_with(
            "example.sharepoint.com", "site_name", "library_name", {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": "Bearer fake_token"
            })
        assert result == expected_base64_image


def test_retrieve_image_as_base64_file_not_found():
    sharepoint_url = "https://example.sharepoint.com/sites/site_name/library_name/file.jpg"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"
    mock_download_url = "https://example.com/download/file.png"
    mock_image_data = b"fake_image_data"
    expected_base64_image = base64.b64encode(mock_image_data).decode("utf-8")

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=404),  # First attempt fails
            Mock(
                status_code=200,
                json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}
            ),  # Second attempt succeeds
            Mock(status_code=200, content=mock_image_data)
        ]

        result = SharePointUtilities.retrieve_image_as_base64(sharepoint_url, token)
        assert result == expected_base64_image


def test_retrieve_image_as_base64_download_failure():
    sharepoint_url = "https://example.sharepoint.com/sites/site_name/library_name/file.jpg"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"
    mock_download_url = "https://example.com/download/file.jpg"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the file metadata
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url}),
            Mock(status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))
        ]

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.retrieve_image_as_base64(sharepoint_url, token)

# endregion

# region Tests for save_file


def test_save_file_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    file_path = "folder/file.txt"
    token = "fake_token"
    file_bytes = b"fake_file_content"
    mock_drive_id = "fake_drive_id"
    mock_response_json = {
        "id": "file_id",
        "webUrl": "https://example.sharepoint.com/sites/site_name/library_name/folder/file.txt"
    }

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.put") as mock_put:

        # Mock the response for uploading the file
        mock_put.return_value = Mock(status_code=200, json=lambda: mock_response_json)

        result = SharePointUtilities.save_file(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, file_path, token, file_bytes
        )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_put.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/items/root:/{file_path}:/content",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            },
            data=file_bytes
        )

        assert result == mock_response_json


def test_save_file_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    file_path = "folder/file.txt"
    token = "fake_token"
    file_bytes = b"fake_file_content"
    mock_drive_id = "fake_drive_id"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.put") as mock_put:

        # Mock the response for uploading the file
        mock_put.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.save_file(
                sharepoint_tenant_fqdn, sharepoint_site_name, library_name, file_path, token, file_bytes
            )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_put.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/items/root:/{file_path}:/content",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            },
            data=file_bytes
        )

# endregion

# region Tests for update_sharepoint_columns


def test_update_sharepoint_columns_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    item_id = "item_id"
    column_updates = {"Title": "New Title"}
    token = "fake_token"
    mock_drive_id = "fake_drive_id"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.patch") as mock_patch:

        # Mock the response for updating the columns
        mock_patch.return_value = Mock(status_code=200)

        SharePointUtilities.update_sharepoint_columns(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, item_id, column_updates, token
        )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_patch.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/items/{item_id}/listItem/fields",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            },
            json=column_updates
        )


def test_update_sharepoint_columns_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    item_id = "item_id"
    column_updates = {"Title": "New Title"}
    token = "fake_token"
    mock_drive_id = "fake_drive_id"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.patch") as mock_patch:

        # Mock the response for updating the columns
        mock_patch.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.update_sharepoint_columns(
                sharepoint_tenant_fqdn, sharepoint_site_name, library_name, item_id, column_updates, token
            )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_patch.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/items/{item_id}/listItem/fields",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            },
            json=column_updates
        )

# endregion

# region Tests for get_file_download_url


def test_get_file_download_url_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    file_name = "file.txt"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"
    mock_download_url = "https://example.com/download/file.txt"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the file download URL
        mock_get.return_value = Mock(status_code=200, json=lambda: {"@microsoft.graph.downloadUrl": mock_download_url})

        result = SharePointUtilities.get_file_download_url(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, file_name, token
        )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/root:/{file_name}"
                "?select=@microsoft.graph.downloadUrl"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            }
        )

        assert result == mock_download_url


def test_get_file_download_url_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    file_name = "file.txt"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the file download URL
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_file_download_url(
                sharepoint_tenant_fqdn, sharepoint_site_name, library_name, file_name, token
            )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/root:/{file_name}"
                "?select=@microsoft.graph.downloadUrl"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            }
        )

# endregion

# region Tests for get_download_urls_for_files_in_folder


def test_get_download_urls_for_files_in_folder_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    folder_name = "folder_name"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"
    mock_files = [
        {"id": "file1", "name": "file1.txt", "content.downloadUrl": "https://example.com/download/file1.txt"},
        {"id": "file2", "name": "file2.txt", "content.downloadUrl": "https://example.com/download/file2.txt"}
    ]

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the files in the folder
        mock_get.return_value = Mock(status_code=200, json=lambda: {"value": mock_files})

        result = SharePointUtilities.get_download_urls_for_files_in_folder(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, folder_name, token
        )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/root:/{folder_name}:/children"
                "?$select=id,name,content.downloadUrl,file"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            }
        )

        assert result == mock_files


def test_get_download_urls_for_files_in_folder_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    folder_name = "folder_name"
    token = "fake_token"
    mock_drive_id = "fake_drive_id"

    with patch(
        "corvus_python.sharepoint.sharepoint_utils.SharePointUtilities._get_drive_id", return_value=mock_drive_id
    ), patch("requests.get") as mock_get:

        # Mock the response for getting the files in the folder
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities.get_download_urls_for_files_in_folder(
                sharepoint_tenant_fqdn, sharepoint_site_name, library_name, folder_name, token
            )

        SharePointUtilities._get_drive_id.assert_called_once_with(
            sharepoint_tenant_fqdn, sharepoint_site_name, library_name, {
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            })

        mock_get.assert_called_once_with(
            (
                f"https://graph.microsoft.com/v1.0/drives/{mock_drive_id}/root:/{folder_name}:/children"
                "?$select=id,name,content.downloadUrl,file"
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}"
            }
        )

# endregion

# region Tests for _get_drive_id


def test_get_drive_id_success():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": "Bearer fake_token"
    }
    mock_drive_id = "fake_drive_id"
    mock_web_url = f"https://{sharepoint_tenant_fqdn}/sites/{sharepoint_site_name}/{library_name}"
    mock_drives_response = {
        "value": [
            {"id": "other_drive_id", "webUrl": "https://example.sharepoint.com/sites/other_site/other_library"},
            {"id": mock_drive_id, "webUrl": mock_web_url}
        ]
    }

    with patch("requests.get") as mock_get:
        # Mock the response for listing the drives
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_drives_response)

        result = SharePointUtilities._get_drive_id(sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        mock_get.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives",
            headers=headers
        )

        assert result == mock_drive_id


def test_get_drive_id_failure():
    sharepoint_tenant_fqdn = "example.sharepoint.com"
    sharepoint_site_name = "site_name"
    library_name = "library_name"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": "Bearer fake_token"
    }

    with patch("requests.get") as mock_get:
        # Mock the response for listing the drives
        mock_get.return_value = Mock(
            status_code=500, raise_for_status=lambda: (_ for _ in ()).throw(requests.exceptions.HTTPError()))

        with pytest.raises(requests.exceptions.HTTPError):
            SharePointUtilities._get_drive_id(sharepoint_tenant_fqdn, sharepoint_site_name, library_name, headers)

        mock_get.assert_called_once_with(
            f"https://graph.microsoft.com/v1.0/sites/{sharepoint_tenant_fqdn}:/sites/{sharepoint_site_name}:/drives",
            headers=headers
        )

# endregion
