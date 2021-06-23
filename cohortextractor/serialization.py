import json

from cohortextractor.query_lang import QueryNode


def serialize_cohort_definition(cohort_class):
    outputs = {
        column: query
        for column, query in get_class_vars(cohort_class)
        if not column.startswith("_")
    }

    # Give every node in the DAG an ID
    nodes = walk_query_dag(outputs.values())
    node_ids = {}
    i = 0
    # Iterating backwards gives a more logical order to the output so that
    # nodes are defined before they are referenced
    for node in reversed(list(nodes)):
        if node not in node_ids:
            i += 1
            node_ids[node] = f"#{i}"

    nodes_as_dicts = {
        node_id: node_as_dict(node, node_ids) for node, node_id in node_ids.items()
    }

    data = {
        "nodes": nodes_as_dicts,
        "outputs": {
            column: attr_as_dict(query, node_ids) for column, query in outputs.items()
        },
    }

    return json.dumps(data, indent=2)


def deserialize_cohort_definition(cohort_json):
    pass


def get_class_vars(cls):
    # Quick hack: what are the vars every class gets assigned
    default_vars = set(dir(type("ArbitraryEmptyClass", (), {})))
    return [(key, value) for key, value in vars(cls).items() if key not in default_vars]


def walk_query_dag(nodes):
    parents = []
    for node in nodes:
        yield node
        for attr in ("source", "value"):
            reference = getattr(node, attr, None)
            if isinstance(reference, QueryNode):
                parents.append(reference)
    if parents:
        yield from walk_query_dag(parents)


def node_as_dict(node, node_ids):
    return {
        "type": type(node).__name__,
        "attrs": {
            key: attr_as_dict(value, node_ids)
            for (key, value) in vars(node).items()
            if not key.startswith("_")
        },
    }


def attr_as_dict(value, node_ids):
    if isinstance(value, QueryNode):
        return {"node": node_ids[value]}
    else:
        return value
