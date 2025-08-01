# Installation
This project uses the UV package manager and GDAL. Here is a small guide to quickly get going

```bash
# Ubuntu 24.04
# Update the package list and install necessary packages
sudo apt-get update && apt-get install -y gdal-bin libgdal-dev proj-bin libproj-dev curl build-essential

# Install Roofer
sudo cp docker/binaries/* /usr/bin

# In case you're trying to generate the dutch dataset, you need the following transformation grids for proj
RUN wget https://cdn.proj.org/nl_nsgi_nlgeo2018.tif -O /usr/local/share/proj/nl_nsgi_nlgeo2018.tif && \
    wget https://cdn.proj.org/nl_nsgi_rdtrans2018.tif -O /usr/local/share/proj/nl_nsgi_rdtrans2018.tif

# Install python environment
curl -LsSf https://astral.sh/uv/install.sh | /bin/sh
export PATH="/root/.local/bin:$PATH"

uv sync
```

# Features
This project provides a set of tools to create 3D buildings from point clouds using the Roofer tool. It includes functionality to:
- Create laz file databases for point clouds.
- Create a footprint database from geographic data.
- Run the Roofer tool to generate 3D buildings from point clouds and footprints.    

# Generating a 3D tileset
## Requirements
- Point clouds in laz format (e.g., AHN4, AHN3).
- Footprint database in geopackage format (e.g., BAG database).
- Roofer tool installed and accessible in the system path.
- Tyler for generating cesium compatible 3D tiles.

## File Scheme support
The script supports file schemes for running roofer tiles, these files are temporarily stored on your harddrive for processing.
Current schemes supported are:
* file:// (for local files)
* azure:// (access azure resources using sas tokens)

## Point Clouds
This program expects each point cloud source to be a directory containing laz files. Once all files are within a single directory, you need to create a laz file database. This can be done using the `createlazindex` command. This will create a index.gpkg file containing all the laz footprints within that directory.

```bash
./.venv/bin/python ./src/main.py createlazindex \
    --destination "azure://https://storageaccount.blob.core.windows.net/pointclouds?sv=<sas_token>" \
    --temporary_directory "tempdir"
```

## Footprint Database
Create a footprint database from BAG data (Netherlands only):

```bash
./.venv/bin/python ./src/main.py createbagdb \
    --temporary_directory "tempdir" \
    --database "footprints.gpkg" \
    --year 2022
```

## Running Roofer for All Tiles
Once we've indexed all the point clouds and created the footprint database, we can run the Roofer tool to generate 3D buildings for all tiles. The following command will process all tiles in the specified grid size and save the results to Azure Blob Storage. You can also save and load the results to/from a local file system by using the `file://` scheme. 

The --pointclouds and --pointclouds_labels options allow you to specify multiple point clouds and their corresponding labels. The pointcloud priority is determined by the order in which they are specified. The first point cloud will be used as the primary source, and subsequent point clouds will be used as fallbacks if the primary source does not contain data for a specific footprint. All pointcloud_low_lod files are used after the primary point cloud sources, the low_lod instructs roofer to treat the data as unsuitable for the region grow algorithm used for reconstructing the point cloud data.

Why use multiple sources? For instance if a building was build at later date then is available in the primary point cloud source, secondary sources can provide a fallback pointcloud that does contain the proper data for that date.

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

## Running a single Roofer Tile
If you want to process a single tile with a specific extent, you can use the `runsingleroofertile` command. This command allows you to specify the extent of the tile you want to process, along with the footprints and point clouds. The same pointcloud logic applies as in the `runallroofertiles` command, where you can specify multiple point clouds and their labels.

```bash
./.venv/bin/python ./src/main.py runsingleroofertile \
    --destination "azure://https://storageaccount.blob.core.windows.net/output/tile.city.json" \
    --footprints "file://footprints.gpkg" \
    --year 2022 \
    --temporary_directory "tempdir" \
    --pointclouds "file://ahn/AHN4" \
    --pointclouds_labels "AHN4" \
    --extent 100000 400000 102000 402000
``` 

## Additional Commands

### Split LAZ Files
Split large LAZ files into smaller, manageable chunks, this is useful for processing large point clouds in parallel. The grid size is in meters and determines the x and y size of each chunk. Temporary files will be stored in the specified temporary directory.

```bash
./.venv/bin/python ./src/main.py splitlaz \
    --input_connection "azure://https://storageaccount.blob.core.windows.net/input?sv=<sas_token>" \
    --output_connection "azure://https://storageaccount.blob.core.windows.net/output?sv=<sas_token>" \
    --temporary_directory "tempdir" \
    --grid_size 1000
```

### Generate 3D Tiles with Tyler
Convert CityJSON to 3D Tiles format:
The metadata.city.json is specific to the Netherlands, we hope to dynamically generate this in the future.

```bash
./.venv/bin/python ./src/main.py tyler \
    --source "azure://https://storageaccount.blob.core.windows.net/cityjson?sv=<sas_token>" \
    --destination "azure://https://storageaccount.blob.core.windows.net/3dtiles?sv=<sas_token>" \
    --temporary_directory "tempdir" \
    --mode "buildings" \
    --metadata_city_json "docker/metadata.city.json"
```

### Sound Analysis (Geluid)
Process building data for sound analysis:

```bash
./.venv/bin/python ./src/main.py geluid \
    --source "azure://https://storageaccount.blob.core.windows.net/buildings?sv=<sas_token>" \
    --destination "azure://https://storageaccount.blob.core.windows.net/geluid?sv=<sas_token>" \
    --temporary_directory "tempdir"
```

### Height Analysis (Hoogte)
Process building data for height analysis:

```bash
./.venv/bin/python ./src/main.py hoogte \
    --source "azure://https://storageaccount.blob.core.windows.net/buildings?sv=<sas_token>" \
    --destination "azure://https://storageaccount.blob.core.windows.net/hoogte?sv=<sas_token>" \
    --temporary_directory "tempdir"
```

# Type checking
uv run mypy main.py


# Contributing
Contributions are welcome! We currently don't have a formal contribution guide, but feel free to open issues or pull requests with improvements or bug fixes.