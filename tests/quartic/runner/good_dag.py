from quartic.make import step, writer

@step
def step1(_: "my_input") -> "my_dataset":
    "First step"
    return writer("Stuff", "Further stuff").json({})

@step
def step2(_: "my_dataset") -> "my_dataset2":
    "Second step"
    return writer("Stuff", "Further stuff").json({})

