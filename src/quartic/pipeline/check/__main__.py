import sys
import importlib
import pprint
import json
from collections import defaultdict
import networkx as nx
import argparse
import os.path
from networkx.drawing.nx_agraph import write_dot
from quartic.utils import QuarticException
from quartic import Quartic

from ..step import Step
from ..dataset import Dataset

def build_dag(steps, default_namespace):
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

def parse_args():
    parser = argparse.ArgumentParser(description="run quartic pipelines")
    parser.add_argument("modules", metavar="MODULE", type=str, nargs="+",
                        help="python module(s) containing build pipeline definitions")
    subparsers = parser.add_subparsers(help="sub-command help", dest="operation")
    subparsers.required = True

    actions = [
        ("run", "run a make"),
        ("graphviz", "output a graphviz file"),
        ("json", "output a json graph file"),
        ("explain", "print what would be done")
    ]
    
    for action, action_help in actions:
        cmd = subparsers.add_parser(action, help=action_help)
        cmd.set_defaults(action=action)
        cmd.add_argument("--check-raw-datasets", dest="check_raw_datasets", action="store_true")
        cmd.add_argument("--namespace", required=True)
        cmd.add_argument("--resume-file", dest="resume_file")

    return parser.parse_args()

# https://stackoverflow.com/questions/2892931/longest-common-substring-from-more-than-two-strings-python
def common_prefix(strings):
    """ Find the longest string that is a prefix of all the strings.
    """
    if not strings:
        return ''
    prefix = strings[0]
    for s in strings:
        if len(s) < len(prefix):
            prefix = prefix[:len(s)]
        if not prefix:
            return ''
        for i in range(len(prefix)):
            if prefix[i] != s[i]:
                prefix = prefix[:i]
                break
    return prefix

def contract_inputs(dag, n):
    predecessors = dag.predecessors(n[0])
    contract = defaultdict(list)
    for pred in predecessors:
        successors = dag.successors(pred)
        if len(successors) == 1 and n[0] in successors:
            contract[pred.namespace].append(pred)
    
    for ns in contract.keys():
        if len(contract[ns]) > 5:
            print(contract[ns])
            prefix = common_prefix([c.dataset_id for c in contract[ns]])
            for n in contract[ns][1:]:
                dag.remove_node(n)
            node = dag.node[contract[ns][0]]
            node["label"] = "{}\n + {} more".format(contract[ns][0], len(contract[ns]) - 1)
            node["style"] = "dotted"

def save_graphviz(g, fname):
    dag = g.copy()
    counter = defaultdict(int)
    for n in dag.nodes(True):
        n[1]["shape"] = "box"
        if dag.in_degree(n[0]) == 0:
            counter["raw"] += 1
            n[1]["color"] = "blue"
        if dag.out_degree(n[0]) == 0:
            n[1]["color"] = "deeppink"

            if dag.in_degree(n[0]) > 0:
                counter["output"] += 1

        if dag.out_degree(n[0]) > 0 and dag.in_degree(n[0]) > 0:
            counter["intermediate"] += 1

    pprint.pprint(counter)

    for n in dag.nodes(True):
        if not dag.has_node(n[0]):
            continue
        contract_inputs(dag, n)

    write_dot(dag, fname)

def save_json(dag, fname):
    blob = {
        "nodes": [
            {
                "data": {
                    "id": str(n),
                    "title": str(n),
                    "type": "raw" if dag.in_degree(n) == 0 else "derived"
                }
            }
            for n in dag.nodes()
        ],
        "edges": [
            {
                "data": {
                    "id": idx,
                    "source": str(e[0]),
                    "target": str(e[1])
                }
            }
            for idx, e in enumerate(dag.edges())
        ]
    }
    json.dump(blob, open(fname, "w"), indent=1)

def load_resume_file(resume_file, default_namespace):
    if resume_file and os.path.exists(resume_file):
        datasets = [Dataset.parse(s).fully_qualified(default_namespace) 
                    for s in json.load(open(resume_file))]

    
        datasets = set(datasets)
        print("skipping datasets: {}".format(pprint.pformat(datasets)))
        return datasets
    else:
        print("Resume file not found. Assuming empty.")
        return set()

def run(action, modules, namespace, check_raw_datasets, resume_file=None):
    steps = []
    for module in modules:
        m = importlib.import_module(module)

        for k, v in m.__dict__.items():
            if isinstance(v, Step):
                steps.append(v)

    # build the DAG and check it
    dag = build_dag(steps, namespace)
    check_dag(dag)
    completed_datasets = load_resume_file(resume_file, namespace)

    # build list of raw datasets and ones to materialise (tsorted)
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

    if action == "graphviz":
        save_graphviz(dag, "graph.dot")
    elif action == "json":
        save_json(dag, "graph.json")
    elif action == "explain":
        pprint.pprint({
            "raw": raw_datasets,
            "materialize": materialise_datasets
        })

    quartic = Quartic("http://{service}.platform:{port}/api/")
    # Optionally check if raw datasets exist before starting
    if check_raw_datasets:
        errors = []
        for ds in raw_datasets:
            dataset = ds.resolve(quartic)
            if not dataset.data_exists():
                errors.append("dataset {} not found".format(dataset))
        if errors:
            raise QuarticException(errors)

    # Run/explain steps
    if action in ["run", "explain"]:
        for step in run_steps:
            step_text = "[{}] {} ".format(step.name, step.description if step.description else "")
            inputs = [i for i in step.inputs()]
            outputs = [o for o in step.outputs()]
            print("{:-<80}".format(step_text))
            print("inputs: {}".format(pprint.pformat(inputs)))
            print("output: {}".format(pprint.pformat(outputs)))
            if action == "run":
                output_dataset = outputs[0].fully_qualified(namespace)
                if not output_dataset in completed_datasets:
                    step.execute(quartic, namespace)
                    completed_datasets = completed_datasets.union(set(outputs))
                    if resume_file:
                        json.dump([str(d) for d in completed_datasets], open(resume_file, "w"), indent=1)
                else:
                    print("> Skipping due to resume file")

def main():
    args = parse_args()
    run(args.action, args.modules, args.namespace, args.check_raw_datasets, args.resume_file)

if __name__ == "__main__":
    main()
