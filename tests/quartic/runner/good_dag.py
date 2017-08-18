from quartic.make import step
from quartic.make.dataset import Writer

class DevNullWriter(Writer):
    def apply(self, *args, **kwargs):
        pass

def writer(name, description):
    return DevNullWriter(name, description)

@step
def step1(_: "my_input") -> "my_dataset":
    "First step"
    return writer("Stuff", "Further stuff").json({})

@step
def step2(_: "my_dataset") -> "my_dataset2":
    "Second step"
    return writer("Stuff", "Further stuff").json({})

