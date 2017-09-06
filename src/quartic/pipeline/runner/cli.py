import argparse
import sys
import json
from quartic.common.quartic import Quartic
from quartic.common.exceptions import (
    ArgumentParserException,
    ModuleNotFoundException,
    MultipleMatchingStepsException,
    NoMatchingStepsException,
    UserCodeExecutionException,
)
from quartic.common import utils

class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserException(self, message)

def parse_args(argv):
    parser = ThrowingArgumentParser(description="Evaluate Quartic Python pipelines")
    parser.add_argument("--execute", metavar="STEP_ID", type=str, help="step id to execute")
    parser.add_argument("--evaluate", metavar="OUPUT_FILE", type=str,
                        help="path of file in which to output steps json")
    parser.add_argument("--exception", metavar="EXCEPTION_FILE", default="exception.json",
                        type=str, help="path of file in which to output error information")
    parser.add_argument("--namespace", metavar="NAMESPACE", type=str,
                        help="path of file in which to output error information")
    parser.add_argument("pipelines", metavar="PIPELINES", type=str, nargs="+",
                        help="one or more paths to python packages containing pipeline code")

    args = parser.parse_args(argv)
    if not (args.execute or args.evaluate) or (args.execute and args.evaluate):
        raise ArgumentParserException(parser, "Must specify either --execute or --evaluate")
    if args.execute and not args.namespace:
        raise ArgumentParserException(parser, "Must specify --namespace with --execute")

    return args

def run_user_code(f):
    try:
        return f()
    except ModuleNotFoundError as e:
        raise ModuleNotFoundException(e.name)
    except Exception as e:
        _, _, tb = sys.exc_info()
        raise UserCodeExecutionException(e, tb)

def main(args):
    if args.execute:
        steps = run_user_code(lambda: utils.get_pipeline_from_args(args.pipelines))
        execute_steps = [step for step in steps if step.get_id() == args.execute]
        quartic = Quartic("http://{service}.platform:{port}/api/")
        if len(execute_steps) > 1:
            raise MultipleMatchingStepsException(args.execute, [step.to_dict() for step in execute_steps])
        elif not execute_steps:
            raise NoMatchingStepsException(args.execute, [step.get_id() for step in steps])
        else:
            run_user_code(lambda: execute_steps[0].execute(quartic, args.namespace))

    elif args.evaluate:
        steps = run_user_code(lambda: utils.get_pipeline_from_args(args.pipelines))
        steps = [step.to_dict() for step in steps]
        json.dump(steps, open(args.evaluate, "w"), indent=1)