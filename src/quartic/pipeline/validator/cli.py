import sys
import click
import pprint
from quartic.common import yaml_utils
from quartic.pipeline.validator import dag_utils

def check_for_config():
    config = yaml_utils.config_path()
    if config is None:
        click.echo("No quartic.yml file found. Generate one using qli init.")
        sys.exit(1)


@click.group()
def cli():
    pass

@cli.command()
def steps():
    check_for_config()
    pprint.pprint(dag_utils.valid_steps())
    return dag_utils.valid_steps()


@cli.command()
def validate():
    check_for_config()
    dag_utils.validate()
    click.echo("Pipelines are valid.")

@cli.command()
def init():
    if yaml_utils.config_path():
        click.echo("quartic.yml exists. Bailing.")
        sys.exit(1)
    else:
        yaml_utils.write_default()
