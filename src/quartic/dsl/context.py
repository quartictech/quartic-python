from quartic.common.log import logger

log = logger(__name__)

class DslContext:
    _instance = None
    def __init__(self):
        self._objects = {}
        self._keys = set()

    def __enter__(self):
        if DslContext._instance:
            raise ValueError("A DSLContext is already open")
        DslContext._instance = self
        return self

    def __exit__(self, etype, value, tb):
        DslContext._instance = None

    def add_object(self, module, o):
        key = module, o.get_id()
        if key in self._objects:
            log.warn("Skipping redefined node: %s (in file %s)", o.get_id(), o.get_file())
        else:
            self._objects[key] = o

    @classmethod
    def register(cls, module, o):
        if cls._instance is None:
            raise ValueError("No DSLContext created")
        cls._instance.add_object(module, o)
        return o

    def objects(self):
        return self._objects

    @classmethod
    def nodes(cls):
        return cls._instance.objects().values()
