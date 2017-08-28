import inspect
import itertools
import zlib
import os.path
import pprint
from .exceptions import QuarticException
from .dataset import Dataset

def step(f):
    return Step(f)

class Step:
    def __init__(self, func, *args, **kwargs):
        self.name = func.__name__
        self.description = func.__doc__
        self._func = func
        self._file = os.path.relpath(inspect.getsourcefile(func))
        source_lines = inspect.getsourcelines(func)
        end_line = source_lines[1] + len(source_lines[0]) - 1
        self._line_range = (source_lines[1], end_line)
        self._inputs = {}
        sig = inspect.signature(func)

        # Input datasets
        for p in sig.parameters:
            ann = sig.parameters[p].annotation
            if ann == inspect.Signature.empty:
                raise QuarticException("Unannotated argument: '{}'".format(p))
            elif isinstance(ann, dict):
                self._inputs[p] = {k:Dataset(v) for k, v in ann.items()}
            else:
                self._inputs[p] = Dataset(ann)

        # Output dataset
        ret = sig.return_annotation
        if ret == inspect.Signature.empty:
            raise QuarticException("No output annotation")
        self._output = Dataset(ret)

    def get_id(self):
        datasets = list(self.inputs()) + list(self.outputs())
        return str(zlib.adler32("\n".join([str(d) for d in datasets]).encode()))

    def get_name(self):
        return self.name

    def inputs(self):
        for v in self._inputs.values():
            if isinstance(v, Dataset):
                yield v
            elif isinstance(v, dict):
                for v2 in v.values():
                    yield v2

    def outputs(self):
        yield self._output

    def datasets(self):
        return itertools.chain(self.inputs(), self.outputs())

    def execute(self, quartic, namespace):
        resolve = lambda d: d.with_namespace(namespace).resolve(quartic)

        kwargs = {}
        for k, v in self._inputs.items():
            if isinstance(v, dict):
                kwargs[k] = {k2: resolve(v2) for k2, v2 in v.items()}
            else:
                kwargs[k] = resolve(v)

        output_writer = self._func(**kwargs)
        output_writer.apply(resolve(self._output))

    def __repr__(self):
        return pprint.pformat(self.to_dict())

    def to_dict(self):
        return {
            "id": self.get_id(),
            "name": self.name,
            "description": self.description,
            "file": self._file,
            "line_range": self._line_range,
            "inputs": list([i.to_json() for i in self.inputs()]),
            "outputs": list([o.to_json() for o in self.outputs()])
        }
