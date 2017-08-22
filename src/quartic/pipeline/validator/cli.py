
import argparse
from .dag_utils import validate, graphviz, json, describe

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

    return parser

def main():
    parser = prep_parser()
    args = parser.parse_args()

    if args.command == "validate":
        validate()
    elif args.graph:
        graphviz()
    elif args.describe:
        describe()
    elif args.json:
        json()

if __name__ == "__main__":
    main()
