from collections import defaultdict
import textwrap

import sqlalchemy

from query import QueryNode


def main():
    from study_definition import Cohort

    cohort = {
        key: value for key, value in get_class_vars(Cohort) if not key.startswith("_")
    }
    sql_queries = build_queries(cohort)
    print("\n\n\n".join(sql_queries))


def build_queries(cohort):
    columns_by_query = defaultdict(set)
    for query_node in cohort.values():
        recurse_query_tree(query_node, columns_by_query)
    table_names = defaultdict(_next_table_name().__next__)
    for query, columns in columns_by_query.items():
        table = table_names[query]
        query_sql = get_query_sql(query, columns, table_names)
        query_sql = textwrap.indent(query_sql, "  ")
        yield f"SELECT * INTO {table} FROM (\n{query_sql}\n) t"
    output_sql = ["SELECT"]
    for output_column, query in cohort.items():
        table = table_names[query.source]
        column = query.kwargs["column"]
        output_sql.append(f"  {table}.{column} AS {output_column}")
    output_sql.append("FROM")
    tables = [table_names[query] for query in columns_by_query]
    primary_table = tables.pop(0)
    output_sql.append(f"  {primary_table}")
    output_sql.extend(
        [
            f"  LEFT JOIN {table} ON {primary_table}.patient_id = {table}.patient_id"
            for table in tables
        ]
    )
    yield "\n".join(output_sql)


def get_class_vars(cls):
    default_vars = set(dir(type("ArbitraryEmptyClass", (), {})))
    return [(key, value) for key, value in vars(cls).items() if key not in default_vars]


def recurse_query_tree(query_node, columns_by_query):
    if query_node.name == "get":
        source_query = query_node.source
        column_name = query_node.kwargs["column"]
        columns_by_query[source_query].add(column_name)
    else:
        all_args = (query_node.source,) + tuple(query_node.kwargs.values())
        children = [arg for arg in all_args if isinstance(arg, QueryNode)]
        for child in children:
            recurse_query_tree(child, columns_by_query)


def _next_table_name():
    n = 0
    while True:
        n += 1
        yield f"#table_{n}"


def get_query_sql(query, columns, table_names):
    # get the table
    # get filters
    # get ordering or aggregation
    node_list = []
    node = query
    while node is not None:
        node_list.append(node)
        node = node.source
    node_list.reverse()
    assert node_list[0].name == "table"
    table = node_list[0].kwargs["table_name"]
    filters = [node.kwargs for node in node_list if node.name == "filter"]
    for filter in filters:
        value = filter["value"]
        if isinstance(value, QueryNode):
            other_table = table_names[value.source]
            column = value.kwargs["column"]
            filter["value"] = sqlalchemy.literal_column(f"{other_table}.{column}")
    aggregations = []
    orderings = []
    return build_sql(table, columns, filters, aggregations, orderings)


def build_sql(table, columns, filters=(), aggregations=(), orderings=()):
    # Build a set listing every column we'll need to touch from `table` to
    # build this query
    all_columns = get_column_references(columns, filters, aggregations, orderings)
    table_expr = get_table_expression(table, all_columns)
    # Get table expressions for any other tables we'll need to join to in order
    # to appply the filters
    other_table_exprs = get_table_references(filters)
    joins = table_expr
    for other_table_expr in other_table_exprs:
        joins = joins.outerjoin(
            other_table_expr, table_expr.c.patient_id == other_table_expr.c.patient_id
        )
    query = sqlalchemy.select([table_expr.c[column] for column in columns])
    query = query.select_from(joins)
    for query_filter in filters:
        query = apply_filter(table_expr, query, query_filter)
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def get_column_references(columns, filters, aggregations, orderings):
    all_columns = set(columns)
    for filter in filters:
        all_columns.add(filter["column"])
    return all_columns


def get_table_references(filters):
    return []


def apply_filter(table_expr, query, query_filter):
    column_name = query_filter["column"]
    operator = query_filter["operator"]
    value = query_filter["value"]
    column = table_expr.c[column_name]
    method = getattr(column, operator)
    return query.where(method(value))


def get_table_expression(table_name, fields):
    table = sqlalchemy.text(table_name)
    table = table.columns(
        sqlalchemy.literal_column("patient_id"),
        *[sqlalchemy.literal_column(field) for field in fields],
    )
    table = table.alias(table_name)
    return table


if __name__ == "__main__":
    main()
