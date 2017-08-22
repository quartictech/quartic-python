import importlib.util
import pprint
import os.path
import networkx as nx
from quartic.utils import QuarticException
from ..step import Step
from .utils import save_graphviz, save_json

def build_dag(steps, default_namespace):
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

def check_dag(dag):
    if not nx.is_directed_acyclic_graph(dag):
        raise QuarticException("graph is not a dag")
    else: return True

def get_module_specs():
    module_specs = []
    for f in os.listdir("."):
        if f.endswith(".py"):
            module_specs.append(
                importlib.util.spec_from_file_location(f.strip('.py'), os.path.abspath(f))
                )
    assert module_specs
    return module_specs

def get_pipeline_steps():
    steps = []
    modules = get_module_specs()
    for mspec in modules:
        m = importlib.util.module_from_spec(mspec)
        mspec.loader.exec_module(m)
        for k, v in m.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)
    assert steps
    return steps

def validate(steps=None):
    if not steps:
        steps = get_pipeline_steps()
    # build the DAG and check it
    dag = build_dag(steps, "local-testing")
    return check_dag(dag)

def graphviz():
    steps = get_pipeline_steps()
    validate(steps)
    dag = build_dag(steps, "local-testing")
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
    steps = get_pipeline_steps()
    validate(steps)
    dag = build_dag(steps, "local-testing")
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

    save_json(dag, "graph.dot")

def describe():
    steps = get_pipeline_steps()
    validate(steps)
    dag = build_dag(steps, "local-testing")
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
