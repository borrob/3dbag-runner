from io import BytesIO, TextIOBase
import logging
from multiprocessing import Queue
import multiprocessing
import os
from pathlib import Path
from queue import Empty
import re
import tempfile
from threading import Thread
from typing import BinaryIO, Generator, Optional
from urllib.parse import urlparse, urlunparse
from azure.storage.blob import BlobClient, ContainerClient, BlobProperties

from .AbstractSchemeFileHandler import AbstractSchemeHandler
from .FileHandle import FileHandle

log = logging.getLogger()

class AzureSchemeFileHandler(AbstractSchemeHandler):
    @staticmethod
    def download_file(uri: str, temporary_directory: Optional[Path], file: Optional[str] = None) -> FileHandle:
        sas_url = uri[8:]

        if file != None:
            sas_url = AzureSchemeFileHandler.navigate(uri, file)[8:]

        parsed_url = urlparse(sas_url)
        _, extension = os.path.splitext(parsed_url.path)

        os.makedirs(str(temporary_directory), exist_ok=True)
        blob_client = BlobClient.from_blob_url(sas_url)

        with tempfile.NamedTemporaryFile(dir=temporary_directory, delete=False, suffix=extension) as f:
            stream = blob_client.download_blob(max_concurrency=10) 
            stream.readinto(f)  
            return FileHandle(Path(f.name), True)

    @staticmethod
    def upload_file_directory(file: Path, uri: str, filename: Optional[str]) -> None:
        if filename != None:
            destination = AzureSchemeFileHandler.navigate(uri, filename)
        else:
            destination = AzureSchemeFileHandler.navigate(uri, file.name)

        AzureSchemeFileHandler.upload_file_direct(file, destination)


    @staticmethod
    def _get_read_buffer(stream: BinaryIO) -> BytesIO:
        # Read & convert text streams into bytes
        if isinstance(stream, TextIOBase):
            text = stream.read()
            data = text.encode('utf-8') # type: ignore
            return BytesIO(data)
        else:
            # Assume binary; rewind to start
            stream.seek(0)
            return stream  # type: ignore

    @staticmethod
    def upload_stream_direct(stream: BinaryIO, uri: str) -> None:
        blob_client = BlobClient.from_blob_url(uri[8:])
        
        log.info("Uploading " + uri[8:])
        blob_client.upload_blob(AzureSchemeFileHandler._get_read_buffer(stream), overwrite=True)

    @staticmethod
    def upload_stream_directory(stream: BinaryIO, uri: str, filename: str) -> None:
        uri = AzureSchemeFileHandler.navigate(uri, filename)
        AzureSchemeFileHandler.upload_stream_direct(stream, uri)

    @staticmethod
    def upload_file_direct(file: Path, uri: str) -> None:
        blob_client = BlobClient.from_blob_url(uri[8:])
        log.info("Uploading " + uri[8:])

        with open(file, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

    @staticmethod
    def _list_files_impl(uri: str, regex: str = '', recursive: bool = False) -> Generator[tuple[str, str]]:
        """
        Internal implementation for listing files in Azure blob storage.
        
        Args:
            uri: Azure URI to list files from
            regex: Optional regex pattern to filter files
            recursive: If True, list files recursively; if False, only list files in the current directory
        """
        # Extract the SAS URI components
        sas_uri = uri[8:]
        parsed_uri = urlparse(sas_uri)

        path_parts = parsed_uri.path.split('/')
        container_name = parsed_uri.path.split('/')[1]  # Container is assumed to be the first segment of the path
        path_prefix: Optional[str] = "".join(path_parts[2:])


        # Compile the regex filter if provided
        pattern = re.compile(regex) if regex else None
        
        # SAS token from the URI (everything after the '?')
        sas_token = parsed_uri.query

        # Get the container client
        container_client = ContainerClient.from_container_url(f"https://{parsed_uri.netloc}/{container_name}?{sas_token}")

        # Walk through the blobs in the container
        # Use delimiter for shallow listing, no delimiter for recursive listing
        if recursive:
            if path_prefix == "":
                path_prefix = None
            blob_iter = container_client.list_blobs(name_starts_with=path_prefix)
        else:
            if path_prefix == None:
                path_prefix = ""

            blob_iter = container_client.walk_blobs(name_starts_with=path_prefix, delimiter='/')

            
        for blob in blob_iter:
            if not isinstance(blob, BlobProperties):
                break
            # Create the full URL with the SAS token
            
            blob_url = f"https://{parsed_uri.netloc}/{container_name}/{blob.name}?{sas_token}"
            
            # If regex is provided, filter files based on it
            if pattern and not pattern.match(blob.name):
                continue
            
            # Prefix with 'azure://' and yield the result
            yield os.path.basename(blob.name), f"azure://{blob_url}"

    @staticmethod
    def list_files_shallow(uri: str, regex: str = '') -> Generator[tuple[str, str]]:
        """List files in the current directory (shallow listing)."""
        return AzureSchemeFileHandler._list_files_impl(uri, regex, recursive=False)

    @staticmethod
    def list_files_recursive(uri: str, regex: str = '') -> Generator[tuple[str, str]]:
        """List files recursively through all subdirectories."""
        return AzureSchemeFileHandler._list_files_impl(uri, regex, recursive=True)
        
    @staticmethod # change to only 
    def get_bytes(uri: str) -> bytes:
        blob_client = BlobClient.from_blob_url(uri[8:])

        stream = blob_client.download_blob()
        return stream.readall()
    
    @staticmethod
    def navigate(uri: str, location: str) -> str:
        parsed_uri = urlparse(uri[8:])
        return f"azure://https://{parsed_uri.netloc}{parsed_uri.path}/{location}?{parsed_uri.query}"
    
    @staticmethod
    def exists(uri: str) -> bool:
        blob_client = BlobClient.from_blob_url(uri[8:])
        return blob_client.exists()

    @staticmethod
    def get_bytes_range(uri: str, offset: int, length: int) -> bytes:
        blob_client = BlobClient.from_blob_url(blob_url=uri)
        stream = blob_client.download_blob(offset=offset, length=length)
        return stream.readall()

    @staticmethod
    def upload_folder(folder: Path, uri: str, recursive: bool = True, consumer_count: int = multiprocessing.cpu_count(), queue_size: int = 128) -> None:
        folder = folder.expanduser().resolve()
        if not folder.is_dir():
            raise ValueError(f"'{folder}' is not a directory")

        sas_url = uri[8:]
        parsed = urlparse(sas_url)
        base_path = parsed.path.rstrip("/")

        task_queue: Queue[tuple[Path, str] | None] = Queue(maxsize=queue_size)

        def _producer() -> None:
            iterator = folder.rglob("*") if recursive else folder.glob("*")
            for local_path in iterator:
                if not local_path.is_file():
                    continue

                rel_path = local_path.relative_to(folder).as_posix()
                task_queue.put((local_path, rel_path))

            for _ in range(consumer_count): # one sentinel per consumer so they all terminate cleanly
                task_queue.put(None)

        def _consumer() -> None:
            while True:
                try:
                    item = task_queue.get(timeout=0.5)
                except Empty:
                    continue  # allows thread to exit promptly on SIGINT

                if item is None:  # sentinel -> shut down
                    break

                local_path, rel_path = item
                blob_path = f"{base_path}/{rel_path}"
                blob_url = urlunparse(
                    (parsed.scheme, parsed.netloc, blob_path, "", parsed.query, "")
                )
                
                blob_client = BlobClient.from_blob_url(blob_url)
                with open(local_path, "rb") as data:
                    log.info(f"Uploading {local_path}")
                    blob_client.upload_blob(data, overwrite=True)

        _consumer_threads: list[Thread] = [Thread(target=_consumer, daemon=True) for i in range(consumer_count)]

        for t in _consumer_threads:
            t.start()

        _producer()        
        
        for t in _consumer_threads:
            t.join()

    @staticmethod
    def get_file_size(uri: str) -> int:
        blob_client = BlobClient.from_blob_url(uri[8:])
        blob_properties = blob_client.get_blob_properties()
        return blob_properties.size