
import importlib
import os
import pkgutil
import subprocess
import sys

import argo
from roofhelper.defaultlogging import setup_logging

log = setup_logging()


def get_kubectl_context() -> str:
    """Get the current kubectl context."""
    try:
        result = subprocess.run(['kubectl', 'config', 'current-context'], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "unknown"


def process_workflows(apply: bool = False) -> None:
    """Generate and optionally apply all workflow files."""
    if apply:
        context = get_kubectl_context()
        # Prompt for confirmation
        response = input(f"Using context '{context}' for adding workflows, are you sure? (y/N): ")
        if response.lower() != 'y':
            log.info("Operation cancelled.")
            return

    workflow_modules = []

    for importer, modname, ispkg in pkgutil.iter_modules(argo.__path__, argo.__name__ + "."):
        if not ispkg:  # Only import modules, not sub-packages
            try:
                module = importlib.import_module(modname)
                # Check if the module has a generate_workflow function
                if hasattr(module, 'generate_workflow') and callable(getattr(module, 'generate_workflow')):
                    workflow_modules.append(module)
            except ImportError:
                # Skip modules that can't be imported
                continue

    action = "Applying and Generating" if apply else "Generating"
    log.info(f"{action} Argo workflows...")

    for module in workflow_modules:
        workflow_name = module.__name__.split('.')[-1].replace("_", "-")
        log.info(f"Generating {workflow_name} workflow...")
        module.generate_workflow()

        if apply:
            # Get the directory where the module is located and construct the workflow file path
            if module.__file__ is None:
                log.error(f"Cannot determine file path for module {workflow_name}")
                continue
            module_dir = os.path.dirname(module.__file__)
            # Navigate to parent directory and then to generated folder
            parent_dir = os.path.dirname(os.path.dirname(module_dir))
            generated_dir = os.path.join(parent_dir, "generated")
            workflow_file = os.path.join(generated_dir, f"{workflow_name}.yaml")

            try:
                log.info(f"Applying {workflow_name} workflow...")
                subprocess.run(['kubectl', 'apply', '-f', workflow_file], capture_output=True, text=True, check=True)
                log.info(f"Successfully applied {workflow_name}")
            except subprocess.CalledProcessError as e:
                log.error(f"Failed to apply {workflow_name}: {e.stderr}")
            except FileNotFoundError:
                log.error(f"Workflow file {workflow_file} not found for {workflow_name}")

    action_past = "applied" if apply else "generated"
    log.info(f"Finished {action_past} Argo workflows.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "apply":
        process_workflows(apply=True)
    else:
        process_workflows(apply=False)
