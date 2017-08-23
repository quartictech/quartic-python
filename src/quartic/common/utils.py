import os
import importlib

def get_module_specs():
    module_specs = []
    for f in os.listdir("."):
        if f.endswith(".py"):
            module_specs.append(
                importlib.util.spec_from_file_location(f.strip('.py'), os.path.abspath(f))
                )
    assert module_specs
    return module_specs
