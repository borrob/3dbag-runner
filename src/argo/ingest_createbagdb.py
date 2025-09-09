from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template

# Create a list to store the futures


@argo_worker()
def workerfunc(destination: str, year: int) -> None:
    import logging
    import os
    from pathlib import Path
    from main import createbagdb
    from roofhelper.defaultlogging import setup_logging
    from roofhelper.io import SchemeFileHandler

    logger = setup_logging(logging.INFO)

    logger.info("Creating index of laz files")
    os.makedirs("/workflow/cache", exist_ok=True)
    createbagdb(Path("/workflow/cache"), Path("/workflow/db.gpkg"), year)

    logger.info("Done creating the database, start uploading")
    handler = SchemeFileHandler(Path("/workflow/cache"))
    handler.upload_file_direct(Path("/workflow/db.gpkg"), destination)

    logger.info("Done")


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="createbagdbdag",
                               arguments=[
                                   Parameter(name="destination", default="azure://https://storageaccount.blob.core.windows.net/container?<sas>"),
                                   Parameter(name="year", default="2021")
    ]) as w:
        # Expose parameters as DAG inputs so tasks use {{inputs.parameters.*}} making template reusable via TemplateRef
        with DAG(name="createbagdbdag", inputs=[Parameter(name="destination"), Parameter(name="year")]):
            # Use template input variables instead of workflow.parameters.*
            queue: Script = workerfunc(arguments={  # type: ignore  # noqa: F841
                "destination": "{{inputs.parameters.destination}}",
                "year": "{{inputs.parameters.year}}"
            })  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
