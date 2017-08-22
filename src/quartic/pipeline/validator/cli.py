
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="run quartic pipelines")
    parser.add_argument("modules", metavar="MODULE", type=str, nargs="+",
                        help="python module(s) containing build pipeline definitions")
    subparsers = parser.add_subparsers(help="sub-command help", dest="operation")
    subparsers.required = True

    actions = [
        ("run", "run a make"),
        ("graphviz", "output a graphviz file"),
        ("json", "output a json graph file"),
        ("explain", "print what would be done")
    ]
    
    for action, action_help in actions:
        cmd = subparsers.add_parser(action, help=action_help)
        cmd.set_defaults(action=action)
        cmd.add_argument("--check-raw-datasets", dest="check_raw_datasets", action="store_true")
        cmd.add_argument("--namespace", required=True)
        cmd.add_argument("--resume-file", dest="resume_file")

    return parser.parse_args()

def main():
    args = parse_args()
    run(args.action, args.modules, args.namespace, args.check_raw_datasets, args.resume_file)

if __name__ == "__main__":
    main()