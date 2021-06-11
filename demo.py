from collections import defaultdict

import sqlalchemy

from query import (
    QueryNode,
    ValueFromRow,
    ValueFromAggregate,
    Column,
    BaseTable,
    FilteredTable,
    Row,
)


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
    queries = get_queries(cohort.values())
    tables = {}
    for n, (query, columns) in enumerate(queries.items()):
        if isinstance(query, ValueFromAggregate):
            columns = {"value"}
        table_name = f"#table_{n}"
        table = get_sqlalchemy_table(table_name, columns)
        tables[query] = table
    sql = []
    for query, columns in queries.items():
        query_expr = get_query_expr(query, columns, tables)
        query_sql = query_to_sql(query_expr)
        temp_table = tables[query].name
        sql.append(
            f"SELECT * INTO {temp_table} FROM (\n{query_sql}\n) t"
        )
    population = cohort.pop("population")
    column, table = get_value_expr(population, tables)
    query = sqlalchemy.select([table.c.patient_id.label("patient_id")]).select_from(table).where(column == True)
    for column_name, node in cohort.items():
        column, table = get_value_expr(node, tables)
        query = include_joined_table(query, table)
        query = query.add_columns(column.label(column_name))
    sql.append(query_to_sql(query))
    return sql


def query_to_sql(query):
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def get_sqlalchemy_table(table_name, columns):
    return sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        sqlalchemy.Column("patient_id"),
        *[sqlalchemy.Column(column) for column in columns],
    )


def get_class_vars(cls):
    default_vars = set(dir(type("ArbitraryEmptyClass", (), {})))
    return [(key, value) for key, value in vars(cls).items() if key not in default_vars]


def get_queries(leaf_nodes):
    queries = defaultdict(set)
    for node in walk_tree(leaf_nodes):
        if isinstance(node, (ValueFromRow, Column)):
            queries[node.source].add(node.column)
        elif isinstance(node, ValueFromAggregate):
            queries[node].add(node.column)
    return queries


def walk_tree(nodes):
    parents = []
    for node in nodes:
        yield node
        for attr in ("source", "value"):
            reference = getattr(node, attr, None)
            if isinstance(reference, QueryNode):
                parents.append(reference)
    if parents:
        yield from walk_tree(parents)


def get_query_expr(query, columns, tables):
    node_list = get_node_list(query)
    base_table, filters, row_selector = node_list[0], node_list[1:-1], node_list[-1]
    assert isinstance(base_table, BaseTable)
    assert all(isinstance(f, FilteredTable) for f in filters)
    assert isinstance(row_selector, (Row, ValueFromAggregate))
    query = _get_query_expr(base_table.name, columns, filters, tables)
    query = apply_row_selector(query, row_selector)
    return query


def _get_query_expr(table_name, columns, filters, tables):
    columns = ["patient_id"] + list(columns)
    table_expr = get_table_expression(table_name)
    column_objs = [table_expr.c[column] for column in columns]
    query = sqlalchemy.select(column_objs).select_from(table_expr)
    for query_filter in filters:
        query = apply_filter(query, query_filter, tables)
    return query


def get_node_list(query):
    node_list = []
    node = query
    while True:
        node_list.append(node)
        if type(node) is BaseTable:
            break
        else:
            node = node.source
    node_list.reverse()
    return node_list


def apply_row_selector(query, row_selector):
    if isinstance(row_selector, Row):
        return apply_row_from_order_by(query, sort_columns=row_selector.sort_columns, descending=row_selector.descending)
    elif isinstance(row_selector, ValueFromAggregate):
        return apply_row_from_aggregate(query, function_name=row_selector.function, column=row_selector.column)
    else:
        raise NotImplementedError(f"{row_selector} not handled yet")


def apply_row_from_order_by(query, sort_columns, descending):
    table_expr = get_primary_table_expr(query)
    column_names = [column.name for column in query.selected_columns]
    order_columns = [table_expr.c[column] for column in sort_columns]
    if descending:
        order_columns = [c.desc() for c in order_columns]
    row_num = (
        sqlalchemy.func.row_number()
        .over(order_by=order_columns, partition_by=table_expr.c.patient_id)
        .label("_row_num")
    )
    query = query.add_columns(row_num)
    subquery = query.alias()
    query = sqlalchemy.select([subquery.c[column] for column in column_names])
    query = query.select_from(subquery).where(subquery.c._row_num == 1)
    return query


def apply_row_from_aggregate(query, function_name, column):
    #table_expr = get_primary_table_expr(query)
    function = getattr(sqlalchemy.func, function_name)
    query = query.with_only_columns([query.selected_columns.patient_id, function(query.selected_columns[column]).label("value")])
    query = query.group_by(query.selected_columns.patient_id)
    return query


def apply_filter(query, query_filter, tables):
    column_name = query_filter.column
    operator = query_filter.operator
    value, other_table = get_value_expr(query_filter.value, tables)
    if other_table is not None:
        query = include_joined_table(query, other_table)
    table_expr = get_primary_table_expr(query)
    column = table_expr.c[column_name]
    method = getattr(column, operator)
    return query.where(method(value))


def get_value_expr(value, tables):
    if isinstance(value, (ValueFromRow, Column)):
        other_table = tables[value.source]
        value_expr = other_table.c[value.column]
        return value_expr, other_table
    elif isinstance(value, ValueFromAggregate):
        other_table = tables[value]
        value_expr = other_table.c.value
        return value_expr, other_table
    else:
        return value, None


def include_joined_table(query, table_expr):
    if table_expr.name in [t.name for t in get_joined_tables(query)]:
        return query
    join = sqlalchemy.join(
        query.froms[0],
        table_expr,
        query.selected_columns.patient_id == table_expr.c.patient_id,
        isouter=True,
    )
    return query.select_from(join)


def get_joined_tables(query):
    tables = []
    from_exprs = [query.froms[0]]
    while from_exprs:
        next_expr = from_exprs.pop()
        if isinstance(next_expr, sqlalchemy.sql.selectable.Join):
            from_exprs.extend([next_expr.left, next_expr.right])
        else:
            tables.append(next_expr)
    return tables


def get_primary_table_expr(query):
    return get_joined_tables(query)[-1]


def get_table_expression(table_name):
    if table_name == "clinical_events":
        return get_sqlalchemy_table(table_name, ["code", "date", "numeric_value"])
    if table_name == "practice_registrations":
        return get_sqlalchemy_table(
            table_name, ["date_start", "date_end", "stp_code"]
        )
    elif table_name == "sgss_sars_cov_2":
        return get_subquery_expression(
            table_name,
            ["date", "positive_result"],
            """
            SELECT patient_id, date, True AS positive FROM sgss_positive
            UNION ALL
            SELECT patient_id, date, False AS positive FROM sgss_negative
            """,
        )
    else:
        raise ValueError(f"Unknown table '{table_name}'")


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
