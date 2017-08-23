
import argparse
from .dag_utils import validate, graphviz, json, describe
from quartic.common.yaml_ops import find_config, write_default

def prep_parser():
    parser = argparse.ArgumentParser(description="Validate Quartic pipelines and DAG.")
    subparser = parser.add_subparsers(help="TBD")
    #hack
    # https://bugs.python.org/issue9253#msg186387
    subparser.required = True
    subparser.dest = 'command'
    subparser.add_parser('validate')
    subparser.add_parser('graph')
    subparser.add_parser('json')
    subparser.add_parser('describe')
    subparser.add_parser('init')

    return parser

def main():
    parser = prep_parser()
    args = parser.parse_args()

    if args.command != "init":
        config = find_config()
        if config is None:
            print("No quartic.yml file found. Generate one using qli init.")
            import sys
            sys.exit(1)

    if args.command == "validate":
        validate()
    elif args.command == "graph":
        graphviz()
    elif args.command == "describe":
        describe()
    elif args.command == "json":
        json()
    elif args.command == "init":
        if find_config():
            print("quartic.yml exists. Bailing.")
            import sys
            sys.exit(1)
        else:
            write_default()

if __name__ == "__main__":
    main()
