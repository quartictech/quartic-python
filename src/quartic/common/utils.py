import os
import sys
import importlib
import pkgutil
from quartic.common.step import Step
from quartic.common.exceptions import QuarticException

def get_python_files(dirs_and_files):
    files = []
    for a in dirs_and_files:
        if os.path.isdir(a):
            files += find_python_files(a)
        if a.endswith(".py"):
            files.append(a)
    if files:
        return files
    else:
        print("No files found for current config. Bailing.") #TODO - log this scenario
        sys.exit(1)

def find_python_files(d):
    files = []
    for f in os.listdir(d):
        if f.endswith(".py"):
            files.append(os.path.join(d, f))
        else:
            continue
    return files

def load_module(mspec):
    m = importlib.util.module_from_spec(mspec)
    mspec.loader.exec_module(m)
    return m

# TODO: Figure out a way to do this more nicely
def load_package(package_dir):
    # First load the root package
    package_name = os.path.basename(package_dir)
    init_py = os.path.join(package_dir, "__init__.py")
    if not os.path.exists(init_py):
        raise QuarticException("Package {} is missing __init__.py".format(package_dir))
    mspec = importlib.util.spec_from_file_location(os.path.dirname(package_dir), init_py)
    m = load_module(mspec)
    yield m

    # This is a bit of a kludge to make sure the submodules can find the root package
    # under a sensible name. e.g. if you have the following:
    #
    # pipelines/
    #   bar/
    #     baz.py
    #   foo.py
    #   foo2.py
    #
    # and foo.py
    # contains the following:
    #
    # import pipelines.foo2
    # import pipelines.bar.baz
    #
    # this ensures that both of these imports can resolve the pipelines package.
    sys.modules[package_name] = m

    # Then load submodules of the package to depth 1
    for pkg in pkgutil.iter_modules(m.__path__):
        # TODO: This is a namedtuple in some versions of Python
        mspec = pkg[0].find_spec(pkg[1])
        m = load_module(mspec)
        yield m

def load_modules(package_dirs):
    for package_dir in package_dirs:
        for m in load_package(package_dir):
            yield m

def get_pipeline_from_args(dirs):
    steps = []
    for module in load_modules(dirs):
        for _, v in module.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)
    return steps
