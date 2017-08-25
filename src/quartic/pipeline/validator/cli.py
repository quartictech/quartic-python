
import click
from quartic.common import yaml_utils
from .dag_utils import validate, graphviz, json, describe

def prep_parser():
    @click.group()
    def cli():
        pass

    @cli.command()
    def validate_pipelines():
        validate()
        click.echo("Pipelines are valid.")

    @cli.command()
    def init():
        if yaml_utils.config_path():
            print("quartic.yml exists. Bailing.")
            import sys
            sys.exit(1)
        else:
            yaml_utils.write_default()

    # subparser.add_parser("validate")
    # subparser.add_parser("graph")
    # subparser.add_parser("json")
    # subparser.add_parser("describe")
    # subparser.add_parser("init")

    return cli

    # if args.command != "init":
    #     config = yaml_utils.config_path()
    #     if config is None:
    #         print("No quartic.yml file found. Generate one using qli init.")
    #         import sys
    #         sys.exit(1)
    #
    # if args.command == "validate":
    #     validate()
    # elif args.command == "graph":
    #     graphviz()
    # elif args.command == "describe":
    #     describe()
    # elif args.command == "json":
    #     json()
    # elif args.command == "init":
    #     if yaml_utils.config_path():
    #         print("quartic.yml exists. Bailing.")
    #         import sys
    #         sys.exit(1)
    #     else:
    #         yaml_utils.write_default()

if __name__ == "__main__":
    cli = prep_parser()
