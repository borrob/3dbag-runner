from hera.workflows import DAG, Parameter, Script
from argo.argodefaults import default_worker, get_workflow_template


@default_worker()
def workerfunc(input: str) -> None:
    import json
    import logging
    from typing import Any
    from pathlib import Path
    from roofhelper.io import SchemeFileHandler
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from roofhelper.defaultlogging import setup_logging

    log = setup_logging(logging.INFO)

    EXPECTED_KEYS = [
        "rf_roof_type",
        "rf_h_ground",
        "identificatie",
        "rf_pc_year",
        "rf_pc_source",
        "oorspronkelijkBouwjaar",
        "documentNummer",
        "documentDatum",
        "status",
        "tijdstipRegistratieLV",
        "tijdstipEindRegistratieLV",
        "rf_roof_elevation_50p",
        "rf_roof_elevation_70p",
        "rf_roof_elevation_min",
        "rf_roof_elevation_max",
    ]

    def check_file(data: dict[Any, Any], name: str) -> None:
        for obj_id, obj in data["CityObjects"].items():
            if obj.get("type") != "Building":
                continue

            attrs = obj.get("attributes", {})
            missing = [k for k in EXPECTED_KEYS if k not in attrs]

            if missing:
                log.info(f"{name} {obj_id}: missing attributes: {', '.join(missing)}")

    handler = SchemeFileHandler(Path("/workflow"))

    def _worker(name: str, uri: str) -> None:
        city_json = handler.get_bytes(uri).decode()
        log.info(f"Validate {uri}")
        data = json.loads(city_json)

        check_file(data, name)

    files = (entry for entry in handler.list_entries_shallow(input, regex="(?i)^.*city\\.json$") if entry.is_file)
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [pool.submit(_worker, entry.name, entry.full_uri) for entry in files]

        for future in as_completed(futures):
            future.result()


def generate_workflow() -> None:
    with get_workflow_template(__name__.split('.')[-1],
                               entrypoint="maindag",
                               arguments=[
                                   Parameter(name="input", default="azure://<sas>")
    ]) as w:
        with DAG(name="maindag", inputs=[Parameter(name="input")]):
            worker: Script = workerfunc(arguments={"input": "{{inputs.parameters.input}}"})  # type: ignore   # noqa: F841

        with open(f"generated/{w.name}.yaml", "w") as f:
            w.to_yaml(f)


if __name__ == "__main__":
    generate_workflow()
