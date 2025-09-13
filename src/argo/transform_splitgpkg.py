from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template


@argo_worker()
def workerfunc(
    source: str,
    destination: str,
    year: str,
    postfix: str
) -> None:
    from main import splitgpkg
    from pathlib import Path

    # Construct file pattern with year and postfix
    file_pattern = f"%s_{year}_{postfix}"
    peildatum = int(year) + 1

    # Construct readme with year
    readme_list = [
        f"##3D Gebouwen {year}##",
        "Het bestand is gebaseerd op:",
        f"- hoogte-informatie uit dense matching van stereo-winterluchtfotografie {year} uit LV Beeldmateriaal (LOD 1.2)",
        "- hoogte-informatie uit AHN (LOD 1.3)",
        f"- BAG (peildatum 01-01-{peildatum})",
        "Het bestand bestaat uit LOD 1.3 gebouwen en is aangevuld met LOD 1.2 gebouwen uit het 3D Basisvoorziening."
    ]

    splitgpkg(
        source=source,
        destination=destination,
        split_source="file:///ahn.json",
        file_pattern=file_pattern,
        readme=readme_list,
        temporary_directory=Path("/workflow")
    )


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="splitgpkgdag",
                               arguments=[
                                   Parameter(name="source", default="azure://<sas>"),
                                   Parameter(name="destination", default="azure://<sas>"),
                                   Parameter(name="year", default="2022"),
                                   Parameter(name="postfix", default="3d_gebouwen")
    ]) as w:
        with DAG(name="splitgpkgdag", inputs=[Parameter(name="source"), Parameter(name="destination"), Parameter(name="year"), Parameter(name="postfix")]):
            queue: Script = workerfunc(arguments={  # type: ignore  # noqa: F841
                "source": "{{inputs.parameters.source}}",
                "destination": "{{inputs.parameters.destination}}",
                "year": "{{inputs.parameters.year}}",
                "postfix": "{{inputs.parameters.postfix}}"
            })  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
