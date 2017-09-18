import shutil
import tempfile
import logging

from .context import DslContext
from .node import Node, Executor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class RawExecutor(Executor):
    def __init__(self, raw_dataset_spec):
        self._raw_dataset_spec = raw_dataset_spec

    def execute(self, context, inputs, output, func):
        raw_path = self._raw_dataset_spec.path
        log.info("Fetching dataset: %s", raw_path)
        response = context.quartic.get_unmanaged(context.namespace, raw_path)
        temp = tempfile.TemporaryFile()
        shutil.copyfileobj(response, temp)
        temp.seek(0)
        dataset = context.resolve(output)
        log.info("Writing dataset to storage: %s", raw_path)
        with dataset.writer(self._raw_dataset_spec.path) as writer:
            writer.raw(temp)

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

def raw(f):
    raw_dataset_spec = f()
    return DslContext.register(Node(f, RawExecutor(raw_dataset_spec)))
