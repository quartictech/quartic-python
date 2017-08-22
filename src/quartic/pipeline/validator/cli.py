
import argparse
from .utils import validate, guff_validate

def prep_parser():
    parser = argparse.ArgumentParser(description="Validate Quartic pipelines and DAG.")
    parser.add_argument('validate')

    return parser

def main():
    parser = prep_parser()
    args = parser.parse_args()
    if args.validate:
        validate()
        # guff_validate()



if __name__ == "__main__":
    main()
