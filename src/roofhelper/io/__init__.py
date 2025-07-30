import os
from pathlib import Path

import urllib.request
from .FileHandle import FileHandle
from .SchemeFileHandler import SchemeFileHandler
from .AzureSchemeFileHandler import AzureSchemeFileHandler
from .FileSchemeFileHandler import FileSchemeFileHandler

def download_if_not_exists(url: str, file: Path) -> None:
    if not os.path.exists(file):
        urllib.request.urlretrieve(url, file)