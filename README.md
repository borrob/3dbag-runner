# Installation
This project uses the UV package manager and GDAL. Here is a small guide to quickly get going

```bash
# Ubuntu 24.04
# Update the package list and install necessary packages
sudo apt-get update && apt-get install -y gdal-bin libgdal-dev proj-bin libproj-dev curl build-essential

# Install Roofer
sudo cp binaries/* /usr/bin

# Install python environment
curl -LsSf https://astral.sh/uv/install.sh | /bin/sh
export PATH="/root/.local/bin:$PATH"

uv sync
```

# Steps for creating roofer tiles
First we need to index all the laz files that are available for roofer, this program assumes all laz files of a certain type or year are within a single folder.
## Create laz file databases

## Create a footprint database
After that we need to create footprint database, Kadaster uses the BAG database as source but for different area's you probably need to make your own.
The footprint database must be a geopackage

## Running roofer
Once you have a footprint and roofer database you can feed the data sources to roofer to create 3d buildings, Using either command 
runsingleroofertile or runallroofertiles you can create 

```bash
./.venv/bin/python ./src/main.py runallroofertiles \
    --filename "buildings_{x}_{y}" \
    --year "2022" \
    --footprints "file://footprints.gpkg" \
    --gridsize 2000 \
    --temporary_directory "tempdir" \
    --pointclouds "file://ahn/AHN4" "file://ahn/AHN3" \
    --pointclouds_labels "AHN4" "AHN3" \
    --pointclouds_low_lod "file://2022" \
    --pointclouds_low_lod_labels "2022" \
    --destination "azure://https://storageaccount.blob.core.windows.net/buildings?sv=<sas_token>" \
    --max_workers 30
```
The script supports file schemes for running roofer tiles, these files are temporarily stored on your harddrive for processing.
Current schemes supported are:
* file:// (for local files)
* azure:// (access azure resources using sas tokens)

# Type checking
uv run mypy main.py
