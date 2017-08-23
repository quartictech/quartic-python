import yaml



default_config = dict(
    version='0.1'
)

def write_default():
    with open('quartic.yml', 'w') as yml_file:
        yaml.dump(default_config, yml_file, default_flow_style=False)
