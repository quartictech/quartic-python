import sys
import click
import networkx as nx
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
    s = dag_utils.valid_steps()
    dag = dag_utils.build_dag(s)
    return dag_utils.valid_steps(dag)

@cli.command()
def validate():
    check_for_config()
    dag_utils.validate()
    click.echo("Pipeline is valid.")

@cli.command()
def status():
    check_for_config()
    graph = dag_utils.get_graph()
    qdag = dag_utils.QuarticDag(graph)
    n_nodes = qdag.num_datasets()
    click.echo(click.style("{} nodes:".format(n_nodes), bold=True))
    click.echo("\t {} input datasets and".format(len(qdag.input_nodes())))
    click.secho("\t {} output datasets.".format(len(qdag.output_nodes())))
    if not qdag.one_step_per_ds():
        click.echo("")
        click.echo(" ".join([click.style("WARNING: ", bold=True, fg="yellow"),
                            "more than one step per output dataset."]))

@cli.command()
def init():
    if yaml_utils.config_path():
        click.echo("quartic.yml exists. Bailing.")
        sys.exit(1)
    else:
        yaml_utils.write_default()
