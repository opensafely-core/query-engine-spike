from collections import defaultdict
import textwrap

import sqlalchemy

from query import QueryNode


def _next_table_name():
    n = 0
    while True:
        n += 1
        yield f"#table_{n}"


table_names = defaultdict(_next_table_name().__next__)


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
    for query, columns in columns_by_query.items():
        table = table_names[query]
        query_sql = get_query_sql(query, columns)
        query_sql = textwrap.indent(query_sql, "  ")
        yield f"SELECT * INTO {table} FROM (\n{query_sql}\n) t"
    output_cols = [f"  {list(table_names.values())[0]}.patient_id AS patient_id"]
    for output_column, query in cohort.items():
        table = table_names[query.source]
        column = query.kwargs["column"]
        output_cols.append(f"  {table}.{column} AS {output_column}")
    output_sql = ["SELECT"]
    output_sql.append(",\n".join(output_cols))
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




def get_query_sql(query, columns):
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
    aggregations = []
    orderings = [node.kwargs for node in node_list if node.name == "order"]
    assert len(orderings) <= 1
    ordering = orderings[0] if orderings else None
    return build_sql(table, columns, filters, aggregations, ordering)


def build_sql(table, columns, filters=(), aggregations=(), ordering=None):
    # Build a set listing every column we'll need to touch from `table` to
    # build this query
    columns = ["patient_id"] + list(columns)
    all_columns = get_column_references(columns, filters, aggregations, ordering)
    table_expr = get_table_expression(table, all_columns)
    # Get table expressions for any other tables we'll need to join to in order
    # to appply the filters
    other_table_exprs = get_table_references(filters)
    joins = table_expr
    for other_table_expr in other_table_exprs:
        joins = joins.outerjoin(
            other_table_expr, table_expr.c.patient_id == other_table_expr.c.patient_id
        )
    column_objs = [table_expr.c[column] for column in columns]
    if ordering:
        order_columns = [table_expr.c[column] for column in ordering["columns"]]
        row_num = sqlalchemy.func.row_number().over(order_by=order_columns, partition_by=table_expr.c.patient_id).label("_row_num")
        column_objs.append(row_num)
    query = sqlalchemy.select(column_objs)
    query = query.select_from(joins)
    for query_filter in filters:
        query = apply_filter(table_expr, query, query_filter)
    if ordering:
        subquery = query.alias()
        query = sqlalchemy.select([subquery.c[column] for column in columns])
        query = query.select_from(subquery).where(subquery.c._row_num == 1)
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def get_column_references(columns, filters, aggregations, ordering):
    all_columns = set(columns)
    for filter in filters:
        all_columns.add(filter["column"])
    return all_columns


def get_table_references(filters):
    tables = set()
    for filter in filters:
        value = filter["value"]
        if isinstance(value, QueryNode):
            other_table = table_names[value.source]
            tables.add(other_table)
    return [get_plain_table_expression(table, ()) for table in tables]


def apply_filter(table_expr, query, query_filter):
    column_name = query_filter["column"]
    operator = query_filter["operator"]
    value = query_filter["value"]
    if isinstance(value, QueryNode):
        other_table = table_names[value.source]
        column = value.kwargs["column"]
        value = sqlalchemy.literal_column(f"{other_table}.{column}")
    column = table_expr.c[column_name]
    method = getattr(column, operator)
    return query.where(method(value))


def get_table_expression(table_name, fields):
    if table_name != "sgss_sars_cov_2":
        return get_plain_table_expression(table_name, fields)
    else:
        return get_subquery_expression(
            table_name,
            fields,
            """
            SELECT patient_id, date, True AS positive FROM sgss_positive
            UNION ALL
            SELECT patient_id, date, False AS positive FROM sgss_negative
            """
        )


def get_plain_table_expression(table_name, fields):
    return sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column("patient_id"),
        *[sqlalchemy.Column(field) for field in fields],
    )


def get_subquery_expression(table_name, fields, query):
    table = sqlalchemy.text(query)
    table = table.columns(
        sqlalchemy.literal_column("patient_id"),
        *[sqlalchemy.literal_column(field) for field in fields],
    )
    table = table.alias(table_name)
    return table


if __name__ == "__main__":
    main()
