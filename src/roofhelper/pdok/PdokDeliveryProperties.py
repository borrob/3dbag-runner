import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

import fiona
from shapely.geometry import Polygon, mapping

from ..io import SchemeFileHandler


PDOK_DELIVERY_SCHEMA = {
    'geometry': 'Polygon',
    'properties': {
        'bladnr': 'str',
        'bag_peildatum': 'int',
        'download_size_bytes': 'int',
        'download_link': 'str',
        'startdatum': 'datetime',
        'einddatum': 'datetime',
    }
}

@dataclass
class PdokDeliveryProperties:
    """Properties for PDOK delivery features."""
    bladnr: str
    bag_peildatum: int
    download_size_bytes: int
    download_link: str
    startdatum: datetime
    einddatum: datetime


def extract_ahn_key_from_filename(filename: str) -> Optional[str]:
    """
    Extracts AHN tile key from filename.
    Expected pattern: <year>/geluid/<type>/<ahn_key>_<year>_3dgeluid_<TYPE>.zip
    Example: 2022/geluid/tin/13bn1_2022_3dgeluid_TIN.zip -> 13bn1
    """
    # Remove directory path and extract just the filename
    basename = os.path.basename(filename)
    
    # Pattern to match AHN filenames
    # Expects: <ahn_key>_<year>_3dgeluid_<TYPE>.zip
    match = re.search(r'^([a-zA-Z0-9]+)_(\d{4})_3dgeluid_[^.]+\.zip$', basename)
    if not match:
        return None
    
    ahn_key = match.group(1).upper()  # Convert to uppercase to match ahn.json keys
    return ahn_key


def create_pdok_index(source_uri: str, ahn_json_path: Path, output_gpkg_path: Path, 
                     download_url_prefix: str,
                     temporary_directory: Optional[Path] = None) -> None:
    """
    Creates a PDOK delivery index geopackage from files in the source directory.
    
    Args:
        source_uri: URI to source directory containing year folders (e.g., "file:///data" or "azure://...")
        ahn_json_path: Path to ahn.json file containing tile geometries
        output_gpkg_path: Path where the output geopackage will be created
        download_url_prefix: URL prefix for download links
        temporary_directory: Optional temporary directory for file operations
    """
    # Initialize file handler
    file_handler = SchemeFileHandler(temporary_directory)
    
    # Load AHN geometry data
    with open(ahn_json_path, 'r') as f:
        ahn_geometries = json.load(f)
    
    # Collect all features to write to geopackage
    features = []
    
    # Expected folder structure: /<year>/geluid/<type>/
    folder_types = ['gebouwen', 'tin', 'bodemblakken']
    
    # List all files in source directory
    for _, file_path in file_handler.list_files(source_uri, regex=r'.*\.zip$'):
        # Parse the file path to extract year and check if it matches expected structure
        # Expected: /<year>/geluid/<type>/<filename>.zip
        path_parts = file_path.strip('/').split('/')
        
        if len(path_parts) < 4:
            continue
            
        # Extract year from path
        try:
            year = int(path_parts[-4])  # Year should be 4 levels up from filename
        except (ValueError, IndexError):
            continue
            
        # Check if path contains 'geluid' and one of the expected types
        if len(path_parts) >= 3 and path_parts[-3] == 'geluid' and path_parts[-2] in folder_types:
            filename = path_parts[-1]
            
            # Extract AHN key from filename
            ahn_key = extract_ahn_key_from_filename(filename)
            if not ahn_key or ahn_key not in ahn_geometries:
                continue
                
            # Get geometry from AHN data
            bbox = ahn_geometries[ahn_key]  # [minx, miny, maxx, maxy]
            geometry = Polygon([
                (bbox[0], bbox[1]),  # bottom-left
                (bbox[2], bbox[1]),  # bottom-right
                (bbox[2], bbox[3]),  # top-right
                (bbox[0], bbox[3]),  # top-left
                (bbox[0], bbox[1])   # close polygon
            ])
            
            # Create start and end dates for the year
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            
            # Get file size (this would require downloading, so we'll set to 0 for now)
            # In a real implementation, you might want to get this from file metadata
            file_size = 0
            
            # Construct download link
            # Remove leading slash if present to avoid double slashes
            relative_path = file_path.lstrip('/')
            download_link = f"{download_url_prefix}{relative_path}"
            
            # Create feature for geopackage
            feature = {
                'geometry': mapping(geometry),
                'properties': {
                    'bladnr': ahn_key,
                    'bag_peildatum': year,
                    'download_size_bytes': file_size,
                    'download_link': download_link,
                    'startdatum': start_date,
                    'einddatum': end_date,
                }
            }
            features.append(feature)
    
    # Write features to geopackage
    if features:
        with fiona.open(output_gpkg_path, 'w', driver='GPKG', schema=PDOK_DELIVERY_SCHEMA, 
                       crs='EPSG:28992', layer='pdok_delivery') as gpkg:
            gpkg.writerecords(features)
        print(f"Created PDOK index with {len(features)} features at {output_gpkg_path}")
    else:
        print("No valid files found matching the expected structure")
