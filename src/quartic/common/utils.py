import os
import importlib
from quartic.common.step import Step

def find_python_files(d):
    files = []
    for f in os.listdir(d):
        if f.endswith(".py"):
            files.append(os.path.join(d, f))
        else:
            continue
    return files

def get_files(dirs_and_files):
    files = []
    for a in dirs_and_files:
        if os.path.isdir(a):
            files += find_python_files(a)
        if a.endswith(".py"):
            files.append(a)
    if files:
        return files
    else:
        import sys
        print("No files found for current config. Bailing.") #TODO - log this scenario
        sys.exit(1)

def get_module_specs(files):
    module_specs = [importlib.util.spec_from_file_location(f.strip(".py"), os.path.abspath(f)) for f in files]
    module_specs = [module for module in module_specs if isinstance(module, importlib.machinery.ModuleSpec)]
    assert module_specs
    return module_specs

def get_pipeline_steps(files):
    steps = []
    modules = get_module_specs(files)
    for mspec in modules:
        m = importlib.util.module_from_spec(mspec)
        mspec.loader.exec_module(m)
        for _, v in m.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)
    assert steps
    return steps

def get_pipeline_from_args(dirs_and_files):
    files = get_files(dirs_and_files)
    return get_pipeline_steps(files)
