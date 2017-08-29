from quartic.common.step import step
from quartic.common.dataset import Writer

class DevNullWriter(Writer):
    def apply(self, *args, **kwargs):
        pass

def writer(name, description):
    return DevNullWriter(name, description)

@step
def step1(_: "an_input") -> "an_output":
    "A description"
    return writer("a_name", "a_description").json({})

@step
def step2(_: "not_output_of_step1") -> "some_other_output":
    "Another description"
    return writer("another_name", "another_description").json({})
