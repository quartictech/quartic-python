from .context import DslContext
from .node import Node, Executor

def step(f):
    return DslContext.register(Node(f, StepExecutor()))

class StepExecutor(Executor):
    def execute(self, context, inputs, output, func):
        kwargs = {}
        for k, v in inputs.items():
            if isinstance(v, dict):
                kwargs[k] = {k2: context.resolve(v2) for k2, v2 in v.items()}
            else:
                kwargs[k] = context.resolve(v)

        output_writer = func(**kwargs)
        output_writer.apply(context.resolve(output))
