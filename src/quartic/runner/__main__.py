import argparse
import sys
import importlib
import json
import traceback
from quartic.make.step import Step
from quartic import Quartic

EXIT_INVALID_ARGS = 1
EXIT_SEVERAL_MATCHING_STEPS = 2
EXIT_NO_MATCHING_STEPS = 3
EXIT_USER_CODE_EXCEPTION = 4

def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate Quartic Python pipelines")
    parser.add_argument("modules", metavar="MODULE", type=str, nargs="+",
                        help="python module(s) containing build pipeline definitions")
    parser.add_argument("--execute", type=str, help="step id to execute")
    parser.add_argument("--evaluate", type=str, help="path of file of output file for steps")
    parser.add_argument("--exception", default="exception.json", type=str, help="path of file to output error information")
    parser.add_argument("--namespace", type=str, help="path of file to output error information")

    args = parser.parse_args()
    if not (args.execute or args.evaluate) or (args.execute and args.evaluate):
        sys.exit(EXIT_INVALID_ARGS)
    return args

def collect_steps(modules):
    steps = []
    for module in args.modules:
        m = importlib.import_module(module)

        for _, v in m.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)
    return steps

def write_exception(fname, etype, code, **kwargs):
    e = {"type": etype}
    e.update(kwargs)
    json.dump(e, open(fname, "w"), indent=1)
    sys.exit(code)

def run_user_code(f, exception_file):
    try:
        return f()
    except Exception as e:
        _, _, tb = sys.exc_info()
        extracted_tb = traceback.extract_tb(tb)
        write_exception(exception_file, "code_exception", EXIT_USER_CODE_EXCEPTION, 
                        exception=traceback.format_exc(), 
                        traceback=traceback.format_tb(tb),
                        file_name=extracted_tb[-1].filename,
                        line_number=extracted_tb[-1].lineno,
                        exception_type=type(e).__name__,
                        args = e.args)

if __name__ == "__main__":
    args = parse_args()

    if args.execute:
        steps = run_user_code(lambda: collect_steps(args.modules), args.exception)
        execute_steps = [step for step in steps if step.get_id() == args.execute]
        quartic = Quartic("http://{service}.platform:{port}/api/")
        if len(execute_steps) > 1:
            write_exception(args.exception, "several_matching_steps", EXIT_SEVERAL_MATCHING_STEPS,
                step_id=args.execute,
                steps=[step.to_dict() for step in execute_steps])
        elif len(execute_steps) == 0:
            write_exception(args.exception, "no_matching_steps", EXIT_NO_MATCHING_STEPS,
                step_id=args.execute)
        else:
            execute_steps[0].execute(quartic, args.namespace)

    elif args.evaluate:
        steps = run_user_code(lambda: collect_steps(args.modules), args.exception)
        steps = [step.to_dict() for step in steps]
        json.dump(steps, open(args.evaluate, "w"), indent=1)




