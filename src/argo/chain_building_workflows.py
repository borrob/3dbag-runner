from hera.workflows import Step, Steps, Parameter
from hera.workflows.models import TemplateRef
from argo.argodefaults import get_workflow_template, argo_worker
from typing import Any  # added for mypy suppression


@argo_worker()
def generate_parameters(folder: str, year: str) -> None:
    import json
    import sys
    import logging
    from roofhelper.io import SchemeFileHandler
    from roofhelper.defaultlogging import setup_logging

    logger = setup_logging(logging.INFO)

    # Parse the folder URI to determine the scheme
    handler = SchemeFileHandler()

    # Generate all the paths using the appropriate navigate function
    parameters = {
        "footprints": handler.navigate(folder, f"{year}/bag{year}.gpkg"),
        "cityjson_destination": handler.navigate(folder, f"{year}/cityjsonraw"),
        "validation_input": handler.navigate(folder, f"{year}/cityjsonraw"),
        "validation_output": handler.navigate(folder, f"{year}/cityjson"),
        "height_source": handler.navigate(folder, f"{year}/cityjson"),
        "height_destination": handler.navigate(folder, f"{year}/hoogte/{year}_NL_3d_geluid_gebouwen.gpkg"),
        "geluid_source": handler.navigate(folder, f"{year}/cityjson"),
        "geluid_destination": handler.navigate(folder, f"{year}/geluid/{year}_3d_hoogtestatistieken_gebouwen.gpkg"),
        "tyler_source": handler.navigate(folder, f"{year}/cityjson"),
        "tyler_intermediate": handler.navigate(folder, f"{year}/intermediate"),
        "tyler_destination": handler.navigate(folder, f"{year}/tyler"),
        "height_split_destination": handler.navigate(folder, f"{year}/hoogte"),
        "geluid_split_destination": handler.navigate(folder, f"{year}/geluid")
    }

    logger.info(f"Generated parameters: {json.dumps(parameters, indent=2)}")
    json.dump(parameters, sys.stdout)


def generate_workflow() -> None:
    """
    Create a chained workflow that executes workflows in the following order:
    1. Parallel: ingest_createbagdb, prepare_indexlazfiles
    2. transform_roofer
    3. validate_fixcityjson
    4. Parallel: transform_height, transform_geluid, transform_tyler
    5. Parallel: transform_splitgpkg for height and geluid outputs
    """
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="chain-steps",
                               arguments=[
                                   # Base parameters
                                   Parameter(name="folder", default="azure://<sas>"),
                                   Parameter(name="year", default="2021"),

                                   # Point cloud data sources
                                   Parameter(name="dsm", default="azure://<sas>"),
                                   Parameter(name="ahn3", default="azure://<sas>"),
                                   Parameter(name="ahn4", default="azure://<sas>")
    ]) as w:

        with Steps(name="chain-steps") as s:
            params_step: Any = generate_parameters(arguments={  # type: ignore
                "folder": w.get_parameter("folder"),
                "year": w.get_parameter("year")
            })

            # Step 1: Run ingest_createbagdb and prepare_indexlazfiles (3x) in parallel
            with s.parallel():
                Step(
                    name="ingest-createbagdb",
                    template_ref=TemplateRef(
                        name="ingest-createbagdb",
                        template="createbagdbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": "{{steps.%s.outputs.result.footprints}}" % params_step.name,
                        "year": w.get_parameter("year")
                    }
                )

                Step(
                    name="prepare-indexlazfiles-dsm",
                    template_ref=TemplateRef(
                        name="prepare-indexlazfiles",
                        template="pointclouddbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": w.get_parameter("dsm")
                    }
                )

                Step(
                    name="prepare-indexlazfiles-ahn3",
                    template_ref=TemplateRef(
                        name="prepare-indexlazfiles",
                        template="pointclouddbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": w.get_parameter("ahn3")
                    }
                )

                Step(
                    name="prepare-indexlazfiles-ahn4",
                    template_ref=TemplateRef(
                        name="prepare-indexlazfiles",
                        template="pointclouddbdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "destination": w.get_parameter("ahn4")
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
                    "footprints": "{{steps.%s.outputs.result.footprints}}" % params_step.name,
                    "year": w.get_parameter("year"),
                    "dsm": w.get_parameter("dsm"),
                    "ahn4": w.get_parameter("ahn4"),
                    "ahn3": w.get_parameter("ahn3"),
                    "destination": "{{steps.%s.outputs.result.cityjson_destination}}" % params_step.name,
                    "workercount": "5"
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
                    "input": "{{steps.%s.outputs.result.validation_input}}" % params_step.name,
                    "output": "{{steps.%s.outputs.result.validation_output}}" % params_step.name
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
                        "source": "{{steps.%s.outputs.result.height_source}}" % params_step.name,
                        "destination": "{{steps.%s.outputs.result.height_destination}}" % params_step.name
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
                        "source": "{{steps.%s.outputs.result.geluid_source}}" % params_step.name,
                        "destination": "{{steps.%s.outputs.result.geluid_destination}}" % params_step.name
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
                        "source": "{{steps.%s.outputs.result.tyler_source}}" % params_step.name,
                        "intermediate": "{{steps.%s.outputs.result.tyler_intermediate}}" % params_step.name,
                        "destination": "{{steps.%s.outputs.result.tyler_destination}}" % params_step.name,
                        "mode": "buildings",
                        "workercount": "5"
                    }
                )

            # Step 5: Split the GPKG files generated by height and geluid transforms
            with s.parallel():
                Step(
                    name="transform-splitgpkg-height",
                    template_ref=TemplateRef(
                        name="transform-splitgpkg",
                        template="splitgpkgdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "source": "{{steps.%s.outputs.result.height_destination}}" % params_step.name,
                        "destination": "{{steps.%s.outputs.result.height_split_destination}}" % params_step.name,
                        "year": w.get_parameter("year")
                    }
                )

                Step(
                    name="transform-splitgpkg-geluid",
                    template_ref=TemplateRef(
                        name="transform-splitgpkg",
                        template="splitgpkgdag",
                        cluster_scope=False
                    ),
                    arguments={
                        "source": "{{steps.%s.outputs.result.geluid_destination}}" % params_step.name,
                        "destination": "{{steps.%s.outputs.result.geluid_split_destination}}" % params_step.name,
                        "year": w.get_parameter("year")
                    }
                )

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
