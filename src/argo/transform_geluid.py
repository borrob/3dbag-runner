from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template


@argo_worker()
def workerfunc(source: str, destination: str, year: int) -> None:
    from main import height_database
    from pathlib import Path
    height_database(source, destination, Path("/workflow"), year, True)


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="geluiddag",
                               arguments=[Parameter(name="source", default="azure://<sas>"),
                                          Parameter(name="destination", default="azure://<sas>"),
                                          Parameter(name="year", default="azure://<sas>")]) as w:
        with DAG(name="geluiddag", inputs=[Parameter(name="source"), Parameter(name="destination"), Parameter(name="year")]):
            queue: Script = workerfunc(arguments={  # type: ignore  # noqa: F841
                "source": "{{inputs.parameters.source}}",
                "destination": "{{inputs.parameters.destination}}",
                "year": "{{inputs.parameters.year}}"
            })  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
