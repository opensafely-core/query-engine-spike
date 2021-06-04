from collections import defaultdict
import textwrap

import sqlalchemy

from query import QueryNode, Value, BaseTable, FilteredTable, RowFromOrderBy


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
        column = query.column
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
    if isinstance(query_node, Value):
        source_query = query_node.source
        column_name = query_node.column
        columns_by_query[source_query].add(column_name)
    else:
        children = []
        for attr in ("source", "value"):
            value = getattr(query_node, attr, None)
            if isinstance(value, QueryNode):
                children.append(value)
        for child in children:
            recurse_query_tree(child, columns_by_query)


def get_query_sql(query, columns):
    # get the table
    # get filters
    # get ordering or aggregation
    node_list = []
    node = query
    while True:
        node_list.append(node)
        if type(node) is BaseTable:
            break
        else:
            node = node.source
    node_list.reverse()
    table = node_list[0].name
    filters = [node for node in node_list if type(node) is FilteredTable]
    aggregations = []
    orderings = [node for node in node_list if type(node) is RowFromOrderBy]
    assert len(orderings) <= 1
    ordering = orderings[0] if orderings else None
    return build_sql(table, columns, filters, aggregations, ordering)


def build_sql(table, columns, filters=(), aggregations=(), ordering=None):
    # Build a set listing every column we'll need to touch from `table` to
    # build this query
    columns = ["patient_id"] + list(columns)
    all_columns = get_column_references(columns, filters, aggregations, ordering)
    table_expr = get_table_expression(table, all_columns)
    column_objs = [table_expr.c[column] for column in columns]
    query = sqlalchemy.select(column_objs).select_from(table_expr)
    for query_filter in filters:
        query = apply_filter(query, query_filter)
    if ordering:
        order_columns = [table_expr.c[column] for column in ordering.sort_columns]
        if ordering.descending:
            order_columns = [c.desc() for c in order_columns]
        row_num = (
            sqlalchemy.func.row_number()
            .over(order_by=order_columns, partition_by=table_expr.c.patient_id)
            .label("_row_num")
        )
        query = query.add_columns(row_num)
        subquery = query.alias()
        query = sqlalchemy.select([subquery.c[column] for column in columns])
        query = query.select_from(subquery).where(subquery.c._row_num == 1)
    return str(query.compile(compile_kwargs={"literal_binds": True}))


def get_column_references(columns, filters, aggregations, ordering):
    all_columns = set(columns)
    for filter in filters:
        all_columns.add(filter.column)
    return all_columns


def apply_filter(query, query_filter):
    table_expr = get_primary_table_expr(query)
    column_name = query_filter.column
    operator = query_filter.operator
    value = query_filter.value
    if isinstance(value, QueryNode):
        other_table = table_names[value.source]
        column = value.column
        value = sqlalchemy.literal_column(f"{other_table}.{column}")
        query = include_joined_table(query, get_plain_table_expression(other_table, ()))
    column = table_expr.c[column_name]
    method = getattr(column, operator)
    return query.where(method(value))


def include_joined_table(query, table_expr):
    if table_expr.name in [t.name for t in get_joined_tables(query)]:
        return query
    main_table_expr = get_primary_table_expr(query)
    join = sqlalchemy.join(
        query.froms[0],
        table_expr,
        main_table_expr.c.patient_id == table_expr.c.patient_id,
        isouter=True,
    )
    return query.select_from(join)


def get_joined_tables(query):
    tables = []
    from_exprs = list(query.froms)
    while from_exprs:
        next_expr = from_exprs.pop()
        if isinstance(next_expr, sqlalchemy.sql.selectable.Join):
            from_exprs.extend([next_expr.left, next_expr.right])
        else:
            tables.append(next_expr)
    return tables


def get_primary_table_expr(query):
    return get_joined_tables(query)[-1]


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
            """,
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


def get_filtered_table_expr(table_expr, output_columns, filters):
    joins_needed = {}
    for filter in filters:
        for other_table_expr, needs_outer_join in get_joins_from_filter(filter):
            needs_outer_join |= joins_needed.get(other_table_expr, False)
            joins_needed[other_table_expr] = needs_outer_join
    join_expr = table_expr
    for other_table_expr, needs_outer_join in joins_needed.items():
        join_expr = join_expr.join(
            other_table_expr,
            table_expr.c.patient_id == other_table_expr.c.patient_id,
            isouter=needs_outer_join,
        )
    column_exprs = [table_expr.c[column] for column in output_columns]
    query = sqlalchemy.select(column_exprs).select_from(join_expr)
    for filter in filters:
        query = apply_filter(query, filter)
    return query


if __name__ == "__main__":
    main()
