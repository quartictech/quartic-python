from functools import reduce
from quartic.pipeline.validator import dag_utils
from quartic.common.exceptions import QuarticException
import pprint

class QuarticDag:
    def __init__(self, nx_graph):
        self.graph = nx_graph
        if not dag_utils.check_dag(self.graph):
            raise QuarticException("Not a DAG.")

    def input_nodes(self):
        in_degrees = self.graph.in_degree()
        return [k for k, v in in_degrees.items() if v == 0]

    def output_nodes(self):
        out_degrees = self.graph.out_degree()
        return [k for k, v in out_degrees.items() if v == 0]

    def steps_per_ds(self):
        output_steps = {}
        for edge in self.graph.edges(data=True):
            step = edge[2]['step']
            for output in step.outputs():
                if output not in output_steps.keys():
                    output_steps[output] = [step]
                else:
                    output_steps[output].append(step)
        return output_steps

    def multi_steps_ds(self):
        s_per_ds = self.steps_per_ds()
        return {k: set(v) for k, v in s_per_ds.items() if len(set(v)) > 1}

    def one_step_per_ds(self):
        output_steps = self.steps_per_ds()
        single_steps = [len(set(v)) == 1 for _, v in output_steps.items()]
        return reduce(lambda a, b: a and b, single_steps)

    def num_datasets(self):
        return self.graph.order()
