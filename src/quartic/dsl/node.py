import inspect
import itertools
import zlib
import os.path
import pprint
from quartic.common.exceptions import QuarticException
from quartic.common.dataset import Dataset

class ExecutionContext:
    def __init__(self, quartic, namespace):
        self.quartic = quartic
        self.namespace = namespace

    def resolve(self, dataset):
        return dataset.with_namespace(self.namespace).resolve(self.quartic)

class Executor:
    def execute(self, context, inputs, output, func):
        pass

    def to_dict(self):
        return {}

class LexicalInfo:
    def __init__(self, file, line_range):
        self.file = file
        self.line_range = line_range

class Node:
    def __init__(self, func, executor, *args, **kwargs):
        self.name = func.__name__
        self.description = func.__doc__
        self._func = func
        self._executor = executor
        source_lines = inspect.getsourcelines(func)
        end_line = source_lines[1] + len(source_lines[0]) - 1
        self._lexical_info = LexicalInfo(
            os.path.relpath(inspect.getsourcefile(func)),
            (source_lines[1], end_line))
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
        in_datasets = sorted([str(ds) for ds in self.inputs()])
        out_datasets = [str(ds) for ds in self.outputs()]
        return str(zlib.adler32("\n".join(in_datasets + out_datasets).encode()))

    def get_file(self):
        return self._lexical_info.file

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
        self._executor.execute(ExecutionContext(quartic, namespace),
                               self._inputs, self._output, self._func)

    def __repr__(self):
        return pprint.pformat(self.to_dict())

    def to_dict(self):
        out = {
            "id": self.get_id(),
            "info": {
                "name": self.name,
                "description": self.description,
                "file": self._lexical_info.file,
                "line_range": self._lexical_info.line_range
            },
            "inputs": list([i.to_json() for i in self.inputs()]),
            "outputs": list([o.to_json() for o in self.outputs()])
        }
        out.update(self._executor.to_dict())
        return out
