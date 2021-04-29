from collections import defaultdict
from dataclasses import dataclass


class frozendict(dict):
    def __hash__(self):
        return id(self)


@dataclass(frozen=True)
class QueryNode:
    name: str
    args: tuple
    kwargs: frozendict


def make_method(name):
    def fn(self, *args, **kwargs):
        kwargs["source"] = self
        return QueryNode(name, args, frozendict(kwargs))

    return fn


METHODS = ["filter", "get", "earliest"]
for method in METHODS:
    setattr(QueryNode, method, make_method(method))


def table(table_name):
    return QueryNode(name="table", args=(table_name,), kwargs=frozendict())


def build_sql(cohort_class):
    cohort_vars = get_class_vars(cohort_class)
    queries = defaultdict(set)
    for output_column, query_node in cohort_vars.items():
        recurse_query_tree(query_node, queries)
    print(queries)


def get_class_vars(cls):
    default_vars = set(dir(type("", (), {})))
    return {key: value for key, value in vars(cls).items() if key not in default_vars}


def recurse_query_tree(query_node, queries):
    if query_node.name == "get":
        column = query_node.args[0]
        queries[query_node.kwargs["source"]].add(column)
    else:
        all_args = query_node.args + tuple(query_node.kwargs.values())
        children = [arg for arg in all_args if isinstance(arg, QueryNode)]
        for child in children:
            recurse_query_tree(child, queries)


class my_cohort:
    has_drug_x = (
        table("medications")
        .filter("date", on_or_before="2020-01-01")
        .earliest()
        .get("code")
    )


build_sql(my_cohort)
