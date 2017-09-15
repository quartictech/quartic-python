
from quartic.incubating import raw, FromBucket
from quartic.dsl.node import Node
from quartic.dsl.context import DslContext

class TestRaw:
    def test_decorator(self):
        with DslContext():
            @raw
            def my_dataset() -> "my_dataset":
                return FromBucket("/some/data")

            assert isinstance(my_dataset, Node)
