from io import BytesIO
import multiprocessing
import os
from pathlib import Path
import re
import shutil
from typing import BinaryIO, Generator, Optional
from urllib.parse import urlparse

from .AbstractSchemeFileHandler import AbstractSchemeHandler
from .FileHandle import FileHandle

class FileSchemeFileHandler(AbstractSchemeHandler):
    @staticmethod
    def _get_local_path(uri: str, filename: Optional[str] = None) -> Path:
        parsed_uri = urlparse(uri)
        if filename != None:
            return Path(os.path.join(parsed_uri.netloc + parsed_uri.path, filename))
        elif parsed_uri.path != "":
            return Path(os.path.join(parsed_uri.netloc + parsed_uri.path))
        else:
            return Path(parsed_uri.netloc)

    @staticmethod
    def download_file(uri: str, _: Optional[Path], file: Optional[str] = None) -> FileHandle:
        return FileHandle(FileSchemeFileHandler._get_local_path(uri, file), False)
    
    @staticmethod
    def list_files(uri: str, regex: Optional[str] = None) -> Generator[tuple[str, str]]:
        path = FileSchemeFileHandler._get_local_path(uri)
        if not os.path.isdir(path):
            raise ValueError(f"The provided uri '{uri}' is not a valid directory.")

        for entry in os.listdir(path):
            full_path = os.path.join(path, entry)
            if os.path.isfile(full_path):
                if regex is not None:
                    if re.match(regex, full_path):
                        yield (entry, "file://" + full_path)
                else:
                    yield (entry, "file://" + full_path)

    @staticmethod
    def upload_file_directory(file: Path, uri: str, filename: Optional[str]) -> None:
        destination = FileSchemeFileHandler._get_local_path(uri, filename)
        os.makedirs(destination, exist_ok=True)
        shutil.copy(file, destination)

    @staticmethod
    def upload_file_direct(file: Path, uri: str) -> None:
        destination = FileSchemeFileHandler._get_local_path(uri)
        shutil.copy(file, destination)

    @staticmethod
    def get_bytes(uri: str) -> bytes:
        source = FileSchemeFileHandler._get_local_path(uri)
        with open(source, "rb") as f:
            return f.read()

    @staticmethod
    def get_bytes_range(uri: str, offset: int, length: int) -> bytes:
        with open(uri, 'rb') as f:
            f.seek(offset)
            return f.read(length)

    @staticmethod
    def navigate(uri: str, location: str) -> str:
        return "file://" + str(FileSchemeFileHandler._get_local_path(uri, location))
    
    @staticmethod
    def exists(uri: str) -> bool:
        return os.path.exists(FileSchemeFileHandler._get_local_path(uri))
    
    @staticmethod
    def upload_folder(folder: Path, uri: str, recursive: bool = True, consumer_count: int = multiprocessing.cpu_count(), queue_size: int = 128) -> None:
        destination = FileSchemeFileHandler._get_local_path(uri)
        shutil.copytree(folder, destination, dirs_exist_ok=True)

    @staticmethod
    def upload_stream_direct(stream: BinaryIO, uri: str) -> None:
        dest_path = FileSchemeFileHandler._get_local_path(uri)
        with open(dest_path, "wb") as out_f:
            shutil.copyfileobj(stream, out_f)

    @staticmethod
    def upload_stream_directory(stream: BinaryIO, uri: str, filename: str) -> None:
        dest_path = FileSchemeFileHandler._get_local_path(uri)
        with open(dest_path, "wb") as out_f:
            shutil.copyfileobj(stream, out_f)