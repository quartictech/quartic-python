import json
from collections import defaultdict
import pprint
from networkx.drawing.nx_agraph import write_dot

def contract_inputs(dag, n):
    predecessors = dag.predecessors(n[0])
    contract = defaultdict(list)
    for pred in predecessors:
        successors = dag.successors(pred)
        if len(successors) == 1 and n[0] in successors:
            contract[pred.namespace].append(pred)

    for ns in contract.keys():
        if len(contract[ns]) > 5:
            for node in contract[ns][1:]:
                dag.remove_node(node)
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
