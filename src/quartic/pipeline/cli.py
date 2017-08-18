import argparse
import sys
import importlib
import json
import traceback
from quartic import Quartic
from .step import Step
from .exceptions import *

class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserException(self, message)

def parse_args(argv=sys.argv[1:]):
    parser = ThrowingArgumentParser(description="Evaluate Quartic Python pipelines")
    parser.add_argument("modules", metavar="MODULE", type=str, nargs="+",
                        help="python module(s) containing build pipeline definitions")
    parser.add_argument("--execute", metavar="STEP_ID", type=str, help="step id to execute")
    parser.add_argument("--evaluate", metavar="OUPUT_FILE", type=str, help="path of file of output file for steps")
    parser.add_argument("--exception", metavar="EXCEPTION_FILE", default="exception.json", 
                        type=str, help="path of file to output error information")
    parser.add_argument("--namespace", metavar="NAMESPACE", type=str, help="path of file to output error information")

    args = parser.parse_args(argv)
    if not (args.execute or args.evaluate) or (args.execute and args.evaluate):
        raise ArgumentParserException(parser, "Must specify either --execute or --evaluate")
    return args

def collect_steps(modules):
    steps = []
    for module in modules:
        m = importlib.import_module(module)

        for _, v in m.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)
    return steps

def run_user_code(f, exception_file):
    try:
        return f()
    except ModuleNotFoundError as e:
        raise ModuleNotFoundException(e.name)
    except Exception as e:
        _, _, tb = sys.exc_info()
        extracted_tb = traceback.extract_tb(tb)
        raise UserCodeExecutionException(
            traceback.format_exc(),
            traceback.format_tb(tb),
            extracted_tb[-1].filename,
            extracted_tb[-1].lineno,
            type(e).__name__,
            e.args
        )

def main(args):
    if args.execute:
        steps = run_user_code(lambda: collect_steps(args.modules), args.exception)
        execute_steps = [step for step in steps if step.get_id() == args.execute]
        quartic = Quartic("http://{service}.platform:{port}/api/")
        if len(execute_steps) > 1:
            raise MultipleMatchingStepsException(args.execute, [step.to_dict() for step in execute_steps])
        elif len(execute_steps) == 0:
            raise NoMatchingStepsException(args.execute)
        else:
            execute_steps[0].execute(quartic, args.namespace)

    elif args.evaluate:
        steps = run_user_code(lambda: collect_steps(args.modules), args.exception)
        steps = [step.to_dict() for step in steps]
        json.dump(steps, open(args.evaluate, "w"), indent=1)
