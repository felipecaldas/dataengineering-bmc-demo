from __future__ import annotations

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from demo.config import settings


def service() -> BlobServiceClient:
    if settings.azure_connection_string:
        return BlobServiceClient.from_connection_string(settings.azure_connection_string)
    if settings.azure_account and settings.azure_key:
        return BlobServiceClient(
            account_url=f"https://{settings.azure_account}.blob.core.windows.net",
            credential=settings.azure_key,
        )
    raise RuntimeError(
        "Azure storage is not configured; set AZURE_STORAGE_CONNECTION_STRING or "
        "AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY"
    )


def _qualified(name: str) -> str:
    relative = name.lstrip("/")
    return f"{settings.azure_prefix}/{relative}" if settings.azure_prefix else relative


def init_container() -> None:
    container = service().get_container_client(settings.azure_container)
    if not container.exists():
        raise RuntimeError(
            f"Azure container {settings.azure_container!r} does not exist; provision it "
            "before running the demo"
        )


def upload_bytes(name: str, content: bytes, content_type: str = "text/plain") -> None:
    client = service().get_blob_client(settings.azure_container, _qualified(name))
    client.upload_blob(
        content,
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )


def download_bytes(name: str) -> bytes:
    client = service().get_blob_client(settings.azure_container, _qualified(name))
    return client.download_blob().readall()


def exists(name: str) -> bool:
    return service().get_blob_client(settings.azure_container, _qualified(name)).exists()


def delete(name: str) -> bool:
    try:
        service().get_blob_client(settings.azure_container, _qualified(name)).delete_blob()
    except ResourceNotFoundError:
        return False
    return True


def list_names(prefix: str = "") -> list[str]:
    container = service().get_container_client(settings.azure_container)
    qualified_prefix = _qualified(prefix)
    root_prefix = f"{settings.azure_prefix}/" if settings.azure_prefix else ""
    return [
        item.name.removeprefix(root_prefix)
        for item in container.list_blobs(name_starts_with=qualified_prefix)
    ]
