import sys
import json
from .cli import main, parse_args
from ..common.exceptions import (ArgumentParserException,
    MultipleMatchingStepsException, NoMatchingStepsException,
    UserCodeExecutionException, ModuleNotFoundException)

def write_exception(fname, etype, e):
    output = {"type": etype}
    output.update({k:v for k, v in e.__dict__.items() if not k.startswith("_")})
    json.dump(output, open(fname, "w"), indent=1)

if __name__ == "__main__":
    try:
        args = parse_args()
        main(args)
    except ArgumentParserException as e:
        e.parser.print_usage()
        sys.exit(1)
    except UserCodeExecutionException as e:
        write_exception(args.exception, type(e).__name__, e)
        raise e._exception
    except (MultipleMatchingStepsException, NoMatchingStepsException,
            ModuleNotFoundException) as e:
        write_exception(args.exception, type(e).__name__, e)
        sys.exit(2)
