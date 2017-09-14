import sys
import click
from quartic.common import yaml_utils
from quartic.pipeline.validator import dag_utils
from quartic.pipeline.validator import qdag as qd
from quartic.pipeline.validator import git

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
    try:
        graph = dag_utils.get_graph()
        if dag_utils.is_valid_dag(graph):
            qdag = qd.QuarticDag(graph)
            if qdag.one_step_per_ds():
                click.secho("Pipeline is valid.", fg="green")
            else:
                click.secho("ERROR", fg="red", bold=True)
                click.echo("The following outputs are defined in multiple steps:")
                msteps = qdag.multi_steps_ds()
                for ds, steps in msteps.items():
                    click.secho("\t{}:".format(ds), bold=True)
                    for s in steps:
                        click.secho("\t\t{} in {}".format(s.get_name(), s.get_file()))
        else:
            click.secho("ERROR", fg="red", bold=True)
            click.echo("The pipeline is not a DAG. Check for cycles.")
    #pylint: disable=W0703
    except Exception as e:
        click.secho(" ".join([click.style("ERROR:", fg="red", bold=True), str(e)]))

@cli.command()
def test():
    click.secho("Submitting changes to Quartic")
    repo = git.get_git_repo()
    new_branch = git.create_test_branch(repo)
    git.commit_push(repo, new_branch)
    click.secho("\n Submitted.", fg="green", bold=True)
    click.secho("Check the interface for details.")

@cli.command()
def status():
    check_for_config()
    try:
        graph = dag_utils.get_graph()
        if not dag_utils.is_valid_dag(graph):
            click.secho("Error", fg="red", bold=True)
            click.echo("The pipeline is not a DAG. Check for cycles.")
            return
        qdag = qd.QuarticDag(graph)
        n_nodes = qdag.num_datasets()
        click.echo(click.style("{} nodes:".format(n_nodes), bold=True))
        click.echo("\t{} input datasets and".format(len(qdag.input_nodes())))
        click.secho("\t{} output datasets.".format(len(qdag.output_nodes())))
        if not qdag.one_step_per_ds():
            click.echo("")
            click.echo("{} {}".format(click.style("WARNING: ", bold=True, fg="yellow"),
                                      "more than one step per output dataset."))
            click.echo("Run validate to debug.")
    #pylint: disable=W0703
    except Exception as e:
        click.secho(" ".join([click.style("ERROR:", fg="red", bold=True), str(e)]))

@cli.command(help="""Setup the repository by generating quartic.yml.
            This should be run in the root of your git repository.""")
def init():
    if yaml_utils.config_path():
        click.echo("quartic.yml exists. Bailing.")
        sys.exit(1)
    else:
        yaml_utils.write_default()
