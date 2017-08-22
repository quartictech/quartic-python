
import argparse
from .utils import validate

def parse_args():
    parser = argparse.ArgumentParser(description="Validate Quartic pipelines and DAG.")
    subparsers = parser.add_subparsers(help="sub-command help", dest="operation")
    subparsers.required = False

    actions = [
        ("graphviz", "Output a graphviz file."),
        ("json", "Output a json graph file."),
        ("explain", "Print what would be done.")
    ]
    
    for action, action_help in actions:
        cmd = subparsers.add_parser(action, help=action_help)
        cmd.set_defaults(action=action)

    return parser.parse_args()

def main():
    args = parse_args()
    validate(args.action)

if __name__ == "__main__":
    main()
