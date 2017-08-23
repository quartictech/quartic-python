import os
import importlib

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
            if a.endswith("/"):
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
