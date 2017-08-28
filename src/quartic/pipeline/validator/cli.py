import sys
import click
import networkx as nx
from quartic.common import yaml_utils
from quartic.pipeline.validator import dag_utils
from quartic.pipeline.validator import qdag as qd

def check_for_config():
    config = yaml_utils.config_path()
    if config is None:
        click.echo("No quartic.yml file found. Generate one using qli init.")
        sys.exit(1)

@click.group()
def cli():
    pass

@cli.command()
def validate():
    check_for_config()
    graph = dag_utils.get_graph()
    if dag_utils.is_valid_dag(graph):
        qdag = qd.QuarticDag(graph)
        if qdag.one_step_per_ds():
            click.secho("Pipeline is valid.", fg="green")
        else:
            click.secho("Error", fg="red")
            click.echo("The following outputs are defined in multiple steps:")
            msteps = qdag.multi_steps_ds()
            for ds, steps in msteps.items():
                click.secho("\t{}:".format(ds), bold=True)
                for s in steps:
                    click.secho("\t\t{} in {}".format(s.get_name(), s.get_file()))

@cli.command()
def status():
    check_for_config()
    graph = dag_utils.get_graph()
    qdag = qd.QuarticDag(graph)
    n_nodes = qdag.num_datasets()
    click.echo(click.style("{} nodes:".format(n_nodes), bold=True))
    click.echo("\t{} input datasets and".format(len(qdag.input_nodes())))
    click.secho("\t{} output datasets.".format(len(qdag.output_nodes())))
    if not qdag.one_step_per_ds():
        click.echo("")
        click.echo(" ".join([click.style("WARNING: ", bold=True, fg="yellow"),
                             "more than one step per output dataset."]))
        click.echo("Run validate to debug.")

@cli.command()
def init():
    if yaml_utils.config_path():
        click.echo("quartic.yml exists. Bailing.")
        sys.exit(1)
    else:
        yaml_utils.write_default()
