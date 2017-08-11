import pprint
import itertools
import inspect
from quartic.utils import raise_if_invalid_coord, QuarticException

def step(f):
    return Step(f)

class Dataset:
    def __init__(self, dataset_id, namespace=None):
        if isinstance(dataset_id, Dataset):
            self.namespace = dataset_id.namespace
            self.dataset_id = dataset_id.dataset_id
        else:
            self.namespace = None if namespace is None else str(namespace)
            self.dataset_id = str(dataset_id)
            raise_if_invalid_coord(self.namespace, self.dataset_id)

    def __repr__(self):
        if self.namespace:
            return "{}::{}".format(self.namespace, self.dataset_id)
        else:
            return "::{}".format(self.dataset_id)

    def __hash__(self):
        return hash((self.namespace, self.dataset_id))

    def __eq__(self, other):
        return (isinstance(other, Dataset) and
                self.namespace == other.namespace and self.dataset_id == other.dataset_id)

    def fully_qualified(self, default_namespace):
        if self.namespace:
            return self
        else:
            return Dataset(self.dataset_id, default_namespace)

    def resolve(self, quartic):
        if not self.namespace:
            raise QuarticException("Dataset not fully qualified: {}".format(self))
        return quartic(self.namespace).dataset(self.dataset_id)

    @staticmethod
    def parse(s):
        bits = s.split("::")
        if not bits[0]:
            return Dataset(bits[1])
        return Dataset(bits[1], bits[0])


class Writer:
    def __init__(self, name, description):
        self._name = name
        self._description = description
        self._exec = lambda f: None

    def parquet(self, df):
        self._exec = lambda f: f.parquet(df)
        return self

    def json(self, df):
        self._exec = lambda f: f.json(df)
        return self

    def apply(self, dataset):
        with dataset.writer(self._name, self._description) as f:
            self._exec(f)


def writer(name, description=None):
    return Writer(name, description)


class Step:
    def __init__(self, func, *args, **kwargs):
        self.name = func.__name__
        self.description = func.__doc__
        self._func = func
        self._file = inspect.getfile(func)
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

    def inputs(self):
        for v in self._inputs.values():
            if isinstance(v, Dataset):
                yield v
            elif isinstance(v, dict):
                for v2 in v.values():
                    yield v2

    # TODO - this is weird now
    def outputs(self):
        yield self._output

    def datasets(self):
        return itertools.chain(self.inputs(), self.outputs())

    def execute(self, quartic, default_namespace):
        resolve = lambda d: d.fully_qualified(default_namespace).resolve(quartic)

        kwargs = {}
        for k, v in self._inputs.items():
            if isinstance(v, dict):
                kwargs[k] = {k2: resolve(v2) for k2, v2 in v.items()}
            else:
                kwargs[k] = resolve(v)

        writer = self._func(**kwargs)
        writer.apply(resolve(self._output))

    def __repr__(self):
        return pprint.pformat({
            "name": self.name,
            "description": self.description,
            "file": self._file,
            "inputs": list(self.inputs()),
            "outputs": list(self.outputs())
        })


