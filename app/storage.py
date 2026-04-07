from azure.storage.blob import BlobServiceClient
from app.config import settings
import os

blob_service = BlobServiceClient.from_connection_string(
    settings.AZURE_STORAGE_CONNECTION_STRING
)
container = blob_service.get_container_client(settings.AZURE_CONTAINER_NAME)

def upload_to_blob(file_path: str, blob_name: str) -> str:
    with open(file_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
    return f"https://{blob_service.account_name}.blob.core.windows.net/{settings.AZURE_CONTAINER_NAME}/{blob_name}"

def download_from_blob(blob_name: str, dest_path: str):
    with open(dest_path, "wb") as f:
        data = container.download_blob(blob_name)
        data.readinto(f)

def delete_from_blob(blob_name: str):
    try:
        container.delete_blob(blob_name)
    except Exception:
        pass  # already deleted, ignore
    