from .context import DslContext
from .node import Node, Executor
from ..common.log import logger
log = logger(__name__)

class RawExecutor(Executor):
    def __init__(self, raw_dataset_spec):
        self._raw_dataset_spec = raw_dataset_spec

    def execute(self, context, inputs, output, func):
        raise NotImplementedError()  # Handled by platform instead, so should never be called

    def to_dict(self):
        return {
            "type": "raw",
            "source": self._raw_dataset_spec.to_dict()
        }

class FromBucket:
    def __init__(self, path):
        self.path = path

    def to_dict(self):
        return {
            "type": "bucket",
            "key": self.path
        }

def raw(name, **kwargs):
    def inner(f):
        raw_dataset_spec = f()
        return DslContext.register(Node(f, RawExecutor(raw_dataset_spec), name, kwargs))
    return inner
