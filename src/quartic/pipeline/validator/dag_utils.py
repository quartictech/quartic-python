import pprint
import networkx as nx
from quartic.common.exceptions import QuarticException
from quartic.pipeline.validator.utils import save_graphviz, save_json
from quartic.common.utils import get_pipeline_from_args
from quartic.common import yaml_utils

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
