from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template

# Create a list to store the futures


@argo_worker()
def workfunc(destination: str) -> None:
    import logging
    from pathlib import Path
    from main import createlazindex
    from roofhelper.defaultlogging import setup_logging
    from roofhelper.io import SchemeFileHandler

    logger = setup_logging(logging.INFO)

    # Check if index.gpkg already exists
    handler = SchemeFileHandler(Path("/workflow"))
    index_path = handler.navigate(destination, "index.gpkg")
    if handler.file_exists(index_path):
        logger.info(f"Index file already exists at {index_path}, skipping prepare-indexlazfiles")
        exit(0)

    createlazindex(destination, "/workflow")


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="pointclouddbdag",
                               arguments=[Parameter(name="destination", default="azure://https://storageaccount.blob.core.windows.net/container/path?<sas>")]) as w:
        # Expose for templateRef reuse
        with DAG(name="pointclouddbdag", inputs=[Parameter(name="destination")]):
            queue: Script = workfunc(arguments={"destination": "{{inputs.parameters.destination}}"})  # type: ignore   # noqa: F841

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
