from quartic.make import step, writer

@step
def my_step() -> "my_dataset":
    return writer("Stuff", "Further stuff").json({})

dsfsdf