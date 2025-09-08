
from roofhelper.defaultlogging import setup_logging

log = setup_logging()

if __name__ == "__main__":
    import importlib
    # List of workflow module names
    workflow_module_names = [
        'createbagdb',
        'fixcityjson',
        'geluid',
        'height',
        'lazdb',
        'lazsplit',
        'pdokupdategeluid',
        'roofer',
        'tyler',
        'validatecityjson',
        'pdokupdatebuildings'
    ]

    # Dynamically import workflow modules
    workflow_modules = []
    for module_name in workflow_module_names:
        module = importlib.import_module(f'argo.{module_name}')
        workflow_modules.append(module)

    log.info("Generating Argo workflows...")

    for module in workflow_modules:
        workflow_name = module.__name__.split('.')[-1]
        log.info(f"Generating {workflow_name} workflow...")
        module.generate_workflow()

    log.info("Finished generating Argo workflows.")
