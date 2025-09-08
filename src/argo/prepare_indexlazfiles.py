from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template

# Create a list to store the futures


@argo_worker()
def workfunc(destination: str) -> None:
    from main import createlazindex
    createlazindex(destination, "/workflow")


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="pointclouddbdag",
                               arguments=[Parameter(name="destination", default="azure://https://storageaccount.blob.core.windows.net/container/path?<sas>")]) as w:
        with DAG(name="pointclouddbdag"):
            queue: Script = workfunc(arguments={"destination": w.get_parameter("destination")})  # type: ignore   # noqa: F841

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
