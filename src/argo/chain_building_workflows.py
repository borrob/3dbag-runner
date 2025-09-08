from hera.workflows import Step, Steps, Parameter
from hera.workflows.models import TemplateRef
from argo.argodefaults import get_workflow_template


def generate_workflow() -> None:
    """
    Create a chained workflow that executes workflows in the following order:
    1. Parallel: ingest_createbagdb, prepare_indexlazfiles
    2. transform_roofer
    3. validate_fixcityjson
    4. Parallel: transform_height, transform_geluid, transform_tyler
    """
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="chain-steps",
                               arguments=[
                                   # Parameters for ingest_createbagdb
                                   Parameter(name="destination", default="azure://https://storageaccount.blob.core.windows.net/container?<sas>"),
                                   Parameter(name="year", default="2021"),

                                   # Parameters for prepare_indexlazfiles
                                   Parameter(name="lazfiles_destination", default="azure://https://storageaccount.blob.core.windows.net/container/path?<sas>"),

                                   # Parameters for transform_roofer
                                   Parameter(name="footprints", default="azure://<sas>"),
                                   Parameter(name="dsm", default="azure://<sas>"),
                                   Parameter(name="ahn4", default="azure://<sas>"),
                                   Parameter(name="ahn3", default="azure://<sas>"),
                                   Parameter(name="cityjson_destination", default="azure://<sas>"),
                                   Parameter(name="worker_count", default="5"),

                                   # Parameters for validate_fixcityjson
                                   Parameter(name="validation_input", default="azure://<sas>"),
                                   Parameter(name="validation_output", default="azure://<sas>"),

                                   # Parameters for transform_height
                                   Parameter(name="height_source", default="azure://<sas>"),
                                   Parameter(name="height_destination", default="azure://<sas>"),

                                   # Parameters for transform_geluid
                                   Parameter(name="geluid_source", default="azure://<sas>"),
                                   Parameter(name="geluid_destination", default="azure://<sas>"),

                                   # Parameters for transform_tyler
                                   Parameter(name="tyler_source", default="azure://<sas>"),
                                   Parameter(name="tyler_intermediate", default="azure://<sas>"),
                                   Parameter(name="tyler_destination", default="azure://<sas>"),
                                   Parameter(name="tyler_mode", default="buildings", enum=["buildings", "terrain"])
    ]) as w:

        with Steps(name="chain-steps") as s:
            # Step 1: Run ingest_createbagdb and prepare_indexlazfiles in parallel
            with s.parallel():
                Step(
                    name="ingest-createbagdb",
                    template_ref=TemplateRef(
                        name="ingest-createbagdb",
                        template="createbagdbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": w.get_parameter("destination"),
                        "year": w.get_parameter("year")
                    }
                )

                Step(
                    name="prepare-indexlazfiles",
                    template_ref=TemplateRef(
                        name="prepare-indexlazfiles",
                        template="pointclouddbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": w.get_parameter("lazfiles_destination")
                    }
                )

            # Step 2: Run transform_roofer after the parallel tasks complete
            Step(
                name="transform-roofer",
                template_ref=TemplateRef(
                    name="transform-roofer",
                    template="rooferdag",
                    cluster_scope=False
                ),
                arguments={
                    "footprints": w.get_parameter("footprints"),
                    "year": w.get_parameter("year"),
                    "dsm": w.get_parameter("dsm"),
                    "ahn4": w.get_parameter("ahn4"),
                    "ahn3": w.get_parameter("ahn3"),
                    "destination": w.get_parameter("cityjson_destination"),
                    "workercount": w.get_parameter("worker_count")
                }
            )

            # Step 3: Run validate_fixcityjson after transform_roofer
            Step(
                name="validate-fixcityjson",
                template_ref=TemplateRef(
                    name="validate-fixcityjson",
                    template="maindag",
                    cluster_scope=False
                ),
                arguments={
                    "input": w.get_parameter("validation_input"),
                    "output": w.get_parameter("validation_output")
                }
            )

            # Step 4: Run transform_height, transform_geluid, and transform_tyler in parallel
            with s.parallel():
                Step(
                    name="transform-height",
                    template_ref=TemplateRef(
                        name="transform-height",
                        template="heightdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "source": w.get_parameter("height_source"),
                        "destination": w.get_parameter("height_destination")
                    }
                )

                Step(
                    name="transform-geluid",
                    template_ref=TemplateRef(
                        name="transform-geluid",
                        template="geluiddag",
                        cluster_scope=False
                    ),
                    arguments={
                        "source": w.get_parameter("geluid_source"),
                        "destination": w.get_parameter("geluid_destination")
                    }
                )

                Step(
                    name="transform-tyler",
                    template_ref=TemplateRef(
                        name="transform-tyler",
                        template="tylerdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "source": w.get_parameter("tyler_source"),
                        "intermediate": w.get_parameter("tyler_intermediate"),
                        "destination": w.get_parameter("tyler_destination"),
                        "mode": w.get_parameter("tyler_mode"),
                        "workercount": w.get_parameter("worker_count")
                    }
                )

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
