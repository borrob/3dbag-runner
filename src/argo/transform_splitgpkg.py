from hera.workflows import DAG, Script, Parameter
from argo.argodefaults import argo_worker, get_workflow_template


@argo_worker()
def workerfunc(
    source: str,
    destination: str,
    year: str
) -> None:
    from main import splitgpkg
    from pathlib import Path

    # Construct file pattern with year
    file_pattern = f"%s_{year}_3d_gebouwen.zip"

    # Construct readme with year
    readme_list = [
        f"##3D Hoogtestatistieken Gebouwen {year}##",
        "Het bestand is gebaseerd op:",
        f"- hoogte-informatie uit dense matching van stereo-winterluchtfotografie {year} uit LV Beeldmateriaal (LOD 1.2)",
        "- hoogte-informatie uit AHN (LOD 1.3)",
        f"- BAG (peildatum 31-12-{year})",
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
                                   Parameter(name="year", default="2022")
    ]) as w:
        with DAG(name="splitgpkgdag"):
            queue: Script = workerfunc(arguments={  # type: ignore  # noqa: F841
                "source": w.get_parameter("source"),
                "destination": w.get_parameter("destination"),
                "year": w.get_parameter("year")
            })  # type: ignore

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
