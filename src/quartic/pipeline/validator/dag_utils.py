import pprint
import networkx as nx
from functools import reduce
from quartic.common.exceptions import QuarticException
from quartic.pipeline.validator.utils import save_graphviz, save_json
from quartic.common.utils import get_pipeline_from_args
from quartic.common import yaml_utils

class QuarticDag:
    def __init__(self, nx_graph):
        self.graph = nx_graph

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

    def one_step_per_ds(self):
        output_steps = self.steps_per_ds()
        single_steps = [len(set(v)) == 1 for _, v in output_steps.items()]
        return reduce(lambda a, b: a and b, single_steps)

    def num_datasets(self):
        return self.graph.order()

def build_dag(steps, default_namespace='default'):
    assert steps
    datasets = set()
    for step in steps:
        for ds in step.datasets():
            datasets.add(ds.fully_qualified(default_namespace))

    g = nx.DiGraph()
    for ds in datasets:
        g.add_node(ds)

    for step in steps:
        for i in step.inputs():
            for o in step.outputs():
                g.add_edge(i.fully_qualified(default_namespace),
                           o.fully_qualified(default_namespace), label=step.name, step=step)

    return g

def get_graph(steps=None):
    cfg = yaml_utils.config()
    if not steps:
        pipeline_dir = yaml_utils.attr_paths_from_config(cfg['pipeline_directory'])
        steps = get_pipeline_from_args(pipeline_dir)
    return build_dag(steps, "local-testing")

def check_one_step_per_ds(dag):
    pass

def check_dag(dag):
    if not nx.is_directed_acyclic_graph(dag):
        raise QuarticException("graph is not a dag")
    else: return True

def valid_steps(steps=None):
    cfg = yaml_utils.config()
    if not steps:
        pipeline_dir = yaml_utils.attr_paths_from_config(cfg['pipeline_directory'])
        steps = get_pipeline_from_args(pipeline_dir)
    # build the DAG and check it
    dag = build_dag(steps, "local-testing")
    check_dag(dag)
    return steps

def valid_dag(steps=None):
    dag = build_dag(valid_steps(steps), "local-testing")
    check_dag(dag)
    return dag

def validate(steps=None):
    return check_dag(valid_dag(steps))

def graphviz():
    dag = valid_dag()
    raw_datasets = []
    materialise_datasets = []
    for node in nx.topological_sort(dag):
        if dag.in_degree(node) == 0:
            raw_datasets.append(node)
        else:
            materialise_datasets.append(node)
    # build list of steps to run in topological order
    run_steps = []
    for ds in materialise_datasets:
        edges = dag.in_edges(ds, True)
        steps = set()
        for edge in edges:
            steps.add(edge[2]["step"])

        assert len(steps) == 1
        run_steps.append(steps.pop())

    save_graphviz(dag, "graph.dot")

def json():
    dag = valid_dag()
    raw_datasets = []
    materialise_datasets = []
    for node in nx.topological_sort(dag):
        if dag.in_degree(node) == 0:
            raw_datasets.append(node)
        else:
            materialise_datasets.append(node)

    # build list of steps to run in topological order
    run_steps = []
    for ds in materialise_datasets:
        edges = dag.in_edges(ds, True)
        steps = set()
        for edge in edges:
            steps.add(edge[2]["step"])

        assert len(steps) == 1
        run_steps.append(steps.pop())

    save_json(dag, "graph.json")

def describe():
    dag = valid_dag()
    raw_datasets = []
    materialise_datasets = []
    for node in nx.topological_sort(dag):
        if dag.in_degree(node) == 0:
            raw_datasets.append(node)
        else:
            materialise_datasets.append(node)

    # build list of steps to run in topological order
    run_steps = []
    for ds in materialise_datasets:
        edges = dag.in_edges(ds, True)
        steps = set()
        for edge in edges:
            steps.add(edge[2]["step"])

        assert len(steps) == 1
        run_steps.append(steps.pop())

    pprint.pprint({
        "raw": raw_datasets,
        "materialize": materialise_datasets
    })
