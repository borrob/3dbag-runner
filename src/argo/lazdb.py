from hera.workflows import DAG, WorkflowTemplate, Script, Parameter

from .argodefaults import argo_worker

# Create a list to store the futures
@argo_worker()
def workfunc(destination: str) -> None:
    from main import createlazindex
    createlazindex(destination, "/workflow")

with WorkflowTemplate(name="pointclouddb",
                      generate_name="pointclouddb-",       
                      entrypoint="pointclouddbdag", 
                      namespace="argo",
                      service_account_name="workflow-runner",
                      image_pull_secrets="acrdddprodman",
                      arguments=[Parameter(name="destination", default="azure://https://storageaccount.blob.core.windows.net/container/path?<sas>")]) as w:
    with DAG(name="pointclouddbdag"):
        queue: Script = workfunc(arguments={"destination": w.get_parameter("destination")}) # type: ignore

    with open("generated/lazdb.yaml", "w") as f:
        w.to_yaml(f)