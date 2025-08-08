from hera.workflows import DAG, WorkflowTemplate, Script, Parameter
from argo.argodefaults import argo_worker

# Create a list to store the futures
@argo_worker()
def workerfunc(source: str, destination: str) -> None:
    from main import height_database
    from pathlib import Path
    height_database(source, destination, Path("/workflow"))

with WorkflowTemplate(name="geluid",
                      generate_name="geluid-",       
                      entrypoint="geluiddag", 
                      namespace="argo",
                      service_account_name="workflow-runner",
                      image_pull_secrets="acrdddprodman",
                      arguments=[Parameter(name="source", default="azure://<sas>"),
                                 Parameter(name="destination", default="azure://<sas>")]) as w:
    with DAG(name="geluiddag"):
            queue: Script = workerfunc(arguments={ # type: ignore
            "source": w.get_parameter("source"),
            "destination": w.get_parameter("destination")
        })  # type: ignore

    with open("generated/geluid.yaml", "w") as f:
        w.to_yaml(f)