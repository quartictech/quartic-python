import sys
import json
from quartic.common.exceptions import (
    ArgumentParserException,
    ModuleNotFoundException,
    MultipleMatchingStepsException,
    NoMatchingStepsException,
    UserCodeExecutionException,
)
from .cli import main, parse_args


def write_exception(fname, etype, exception):
    output = {"type": etype}
    for k, v in exception.__dict__.items():
        if not k.startswith("_"):
            try:
                json.dumps(v)
                output[k] = v
            except TypeError:
                output[k] = str(v)
    json.dump(output, open(fname, "w"), indent=1)

if __name__ == "__main__":
    try:
        args = parse_args(sys.argv[1:])
        main(args)
    except ArgumentParserException as e:
        e.parser.print_usage()
        sys.exit(1)
    except UserCodeExecutionException as e:
        write_exception(args.exception, type(e).__name__, e)
        raise e.exception()
    except (MultipleMatchingStepsException, NoMatchingStepsException,
            ModuleNotFoundException) as e:
        write_exception(args.exception, type(e).__name__, e)
        sys.exit(2)
