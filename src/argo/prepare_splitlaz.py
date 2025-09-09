from hera.workflows import DAG, Parameter, Artifact, Script
from argo.argodefaults import argo_worker, get_workflow_template


@argo_worker(outputs=Artifact(name="queue", path="/workflow/queue.json"))
def workerfunc(source: str, destination: str, gridsize: int) -> None:
    from main import pointcloudsplit
    from pathlib import Path
    pointcloudsplit(source, destination, gridsize, Path("/workflow"))


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="maindag",
                               arguments=[
                                   Parameter(name="source", default="https://storageaccount.blob.core.windows.net/container/pointcloud?<sas>"),
                                   Parameter(name="destination", default="https://storageaccount.blob.core.windows.net/container/output?<sas>"),
                                   Parameter(name="grid-size", default="250"),
    ]) as w:
        with DAG(name="maindag", inputs=[Parameter(name="source"), Parameter(name="destination"), Parameter(name="grid-size")]):
            worker: Script = workerfunc(arguments={"source": "{{inputs.parameters.source}}",  # type: ignore  # noqa: F841
                                                   "destination": "{{inputs.parameters.destination}}",
                                                   "gridsize": "{{inputs.parameters.grid-size}}"})  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
