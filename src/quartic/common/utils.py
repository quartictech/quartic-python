import os
import importlib
from quartic.common.step import Step

def find_python_files(d):
    files = []
    for f in os.listdir(d):
        if f.endswith(".py"):
            files.append(f)
        else:
            continue
    return files

def get_files(*args):
    files = []
    if args:
        for a in args:
            if os.path.isdir(a):
                files.append(find_python_files(a))
            if a.endswith(".py"):
                files.append(a)
    else:
        for f in os.listdir("."):
            if f.endswith(".py"):
                files.append(f)
    return files

def get_module_specs(files):
    module_specs = []
    for f in files:
        module_specs.append(
            importlib.util.spec_from_file_location(f.strip('.py'), os.path.abspath(f))
            )
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
