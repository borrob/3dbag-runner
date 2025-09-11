from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template


@argo_worker()
def workerfunc(source: str, destination: str) -> None:
    import multiprocessing
    from roofhelper.io import SchemeFileHandler
    from roofhelper.defaultlogging import setup_logging
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from typing import Any

    log = setup_logging()

    def process_file(entry: Any, file_handler: SchemeFileHandler, destination: str) -> bool:
        """Process a single file: get bytes, create zip, upload, cleanup"""
        try:
            log.info(f"Processing file: {entry.name}")

            # Get file bytes
            file_bytes = file_handler.get_bytes(entry.full_uri)

            # Create zip filename by replacing .city.json with .zip
            zip_filename = entry.name.replace('.city.json', '.zip')

            # Create zip file in memory
            import zipfile
            from io import BytesIO

            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(entry.name, file_bytes)

            # Upload zip directly from memory
            log.info(f"Uploading {zip_filename} to destination")
            zip_buffer.seek(0)
            file_handler.upload_bytes_directory(zip_buffer, destination, zip_filename)

            log.info(f"Successfully processed {entry.name} -> {zip_filename}")
            return True

        except Exception as e:
            log.error(f"Failed to process {entry.name}: {e}")
            return False

    # Initialize SchemeFileHandler with default temporary directory
    file_handler = SchemeFileHandler()

    # List files from source folder (shallow listing) - filter for .city.json files
    log.info(f"Listing .city.json files from source: {source}")
    entries = list(file_handler.list_entries_shallow(source, r'.*\.city\.json$'))

    # All entries should be files ending with .city.json
    file_entries = [entry for entry in entries if entry.is_file]
    log.info(f"Found {len(file_entries)} .city.json files to process")

    if not file_entries:
        log.warning("No files found in source directory")
        exit(-1)

    # Process files in parallel using CPU count
    cpu_count = multiprocessing.cpu_count()
    log.info(f"Processing files with {cpu_count} threads")

    success_count = 0
    with ThreadPoolExecutor(max_workers=cpu_count) as executor:
        # Submit all tasks
        future_to_entry = {
            executor.submit(process_file, entry, file_handler, destination): entry
            for entry in file_entries
        }

        # Wait for completion
        for future in as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                success = future.result()
                if success:
                    success_count += 1
            except Exception as e:
                log.error(f"Task for {entry.name} generated an exception: {e}")

    log.info(f"Workflow completed. Successfully processed {success_count}/{len(file_entries)} files")


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="zipdag",
                               arguments=[Parameter(name="source", default="azure://<sas>"),
                                          Parameter(name="destination", default="azure://<sas>")]) as w:
        with DAG(name="zipdag", inputs=[Parameter(name="source"), Parameter(name="destination")]):
            queue: Script = workerfunc(arguments={  # type: ignore  # noqa: F841
                "source": "{{inputs.parameters.source}}",
                "destination": "{{inputs.parameters.destination}}"
            })  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
