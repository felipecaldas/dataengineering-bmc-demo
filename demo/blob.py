from __future__ import annotations

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from demo.config import settings


def service() -> BlobServiceClient:
    # Keep the local emulator contract stable even when a newer SDK defaults to a
    # storage-service API version that the pinned Azurite image does not emulate.
    return BlobServiceClient.from_connection_string(
        settings.azure_connection_string,
        api_version="2023-11-03",
    )


def init_container() -> None:
    try:
        service().create_container(settings.azure_container)
    except ResourceExistsError:
        pass


def upload_bytes(name: str, content: bytes, content_type: str = "text/plain") -> None:
    client = service().get_blob_client(settings.azure_container, name)
    client.upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )


def download_bytes(name: str) -> bytes:
    client = service().get_blob_client(settings.azure_container, name)
    return client.download_blob().readall()


def exists(name: str) -> bool:
    return service().get_blob_client(settings.azure_container, name).exists()


def delete(name: str) -> bool:
    try:
        service().get_blob_client(settings.azure_container, name).delete_blob()
    except ResourceNotFoundError:
        return False
    return True


def list_names(prefix: str = "") -> list[str]:
    container = service().get_container_client(settings.azure_container)
    return [blob.name for blob in container.list_blobs(name_starts_with=prefix)]
