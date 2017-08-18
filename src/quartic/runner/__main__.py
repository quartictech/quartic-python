import sys
import json
from .runner import (main, parse_args, ArgumentParserException,
    MultipleMatchingStepsException, NoMatchingStepsException,
    UserCodeExecutionException, ModuleNotFoundException)

def write_exception(fname, etype, code, e):
    output = {"type": etype}
    output.update(e.__dict__)
    json.dump(output, open(fname, "w"), indent=1)
    sys.exit(code)

if __name__ == "__main__":
    try:
        args = parse_args()
        main(args)
    except ArgumentParserException as e:
        e.parser.print_usage()
        sys.exit(e.exit_code)
    except (MultipleMatchingStepsException, NoMatchingStepsException, UserCodeExecutionException,
            ModuleNotFoundException) as e:
        write_exception(args.exception, type(e).__name__, e.exit_code, e)

