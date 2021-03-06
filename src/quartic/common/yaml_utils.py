import os
import yaml

pipeline_directory = "pipeline_directory"

default_config = dict(
    version="0.1",
    pipeline_directory="pipelines"
)

def write_default(path=None):
    if path is None:
        path = os.getcwd()
    with open("quartic.yml", "w") as yml_file:
        yaml.dump(default_config, yml_file, default_flow_style=False)

def load_config(cfg):
    with open(cfg, "r") as yml_file:
        return yaml.load(yml_file)

def config(path=None):
    if path is None:
        path = os.getcwd()
    return load_config(config_path(path))

def config_path(path=None):
    if path is None:
        path = os.getcwd()
    if "quartic.yml" in os.listdir(path):
        return os.path.abspath(os.path.join(path, "quartic.yml"))
    else:
        parent_dir = os.path.join(path, os.pardir)
        if os.path.realpath(parent_dir) == os.path.realpath(path):
            return None
        elif os.access(parent_dir, os.R_OK) and os.access(parent_dir, os.X_OK):
            return config_path(parent_dir)

def attr_paths_from_config(attribute):
    """Get list of paths for an attribute from the config
    file."""
    cfg_path = config_path()
    return [os.path.abspath(
        os.path.join(os.path.dirname(cfg_path), attribute))]
