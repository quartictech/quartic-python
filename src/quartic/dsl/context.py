class DslContext:
    _instance = None
    def __init__(self):
        self._objects = []

    def __enter__(self):
        if DslContext._instance:
            raise ValueError("A DSLContext is already open")
        DslContext._instance = self
        return self

    def __exit__(self, etype, value, tb):
        DslContext._instance = None

    @classmethod
    def register(cls, o):
        if cls._instance is None:
            raise ValueError("No DSLContext created")
        cls._instance.objects().append(o)
        return o

    def objects(self):
        return self._objects

    @classmethod
    def nodes(cls):
        return cls._instance.objects()
