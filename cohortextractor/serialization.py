from cohortextractor.query_lang import (
    QueryNode,
    BaseTable,
    FilteredTable,
    Row,
    Column,
    ValueFromRow,
    ValueFromAggregate,
)


# Map each class in the query DAG to a (type, operation) pair to avoid leaking
# the classes into the serialized structure
CLASS_MAP = {
    BaseTable: ("table", "base_table"),
    FilteredTable: ("table", "filter"),
    Column: ("colum", "table"),
    Row: ("row", "sort"),
    ValueFromRow: ("value", "row"),
    ValueFromAggregate: ("value", "aggregation"),
}

CLASS_MAP_INVERSE = dict(zip(CLASS_MAP.values(), CLASS_MAP.keys()))
assert len(CLASS_MAP) == len(CLASS_MAP_INVERSE), "CLASS_MAP must be invertable"


def cohort_definition_to_dict(cohort_definition):
    # Give every node in the DAG an ID
    nodes = walk_query_dag(cohort_definition.values())
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

    return {
        "nodes": nodes_as_dicts,
        "outputs": {
            column: attr_as_dict(query, node_ids)
            for column, query in cohort_definition.items()
        },
    }


def cohort_definition_from_dict(data):
    node_defs = data["nodes"]
    nodes = {}
    return {
        column: attr_from_dict(value, node_defs, nodes)
        for (column, value) in data["outputs"].items()
    }


def cohort_class_to_definition(cohort_class):
    return {
        column: query
        for column, query in get_class_vars(cohort_class)
        if not column.startswith("_")
    }


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
    type_, operation = CLASS_MAP[node.__class__]
    return {
        "type": type_,
        "from": operation,
        "attrs": {
            key: attr_as_dict(value, node_ids)
            for (key, value) in node.to_dict().items()
        },
    }


def node_from_dict(node_id, node_defs, nodes):
    node_def = node_defs[node_id]
    class_id = (node_def["type"], node_def["from"])
    node_class = CLASS_MAP_INVERSE[class_id]
    attrs = {
        key: attr_from_dict(value, node_defs, nodes)
        for (key, value) in node_def["attrs"].items()
    }
    return node_class.from_dict(attrs)


def attr_as_dict(value, node_ids):
    if isinstance(value, QueryNode):
        return {"node": node_ids[value]}
    else:
        return value


def attr_from_dict(value, node_defs, nodes):
    if isinstance(value, dict):
        node_id = value["node"]
        if node_id in nodes:
            return nodes[node_id]
        else:
            node = node_from_dict(node_id, node_defs, nodes)
            nodes[node_id] = node
            return node
    else:
        return value
