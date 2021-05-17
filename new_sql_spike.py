import textwrap

import sqlalchemy


"""
class DatabaseDefinition:

    sgss = Table(
        patient_id=PatientIDColumn(),
    )
"""


def get_table_expression(table_name, fields):
    table = sqlalchemy.text(
        textwrap.dedent(
            """
        SELECT patient_id, date, True AS positive FROM sgss_positive
        UNION ALL
        SELECT patient_id, date, False AS positive FROM sgss_negative
        """
        )
    )
    table = table.columns(
        sqlalchemy.literal_column("patient_id"),
        sqlalchemy.literal_column("date"),
        sqlalchemy.literal_column("positive"),
    )
    table = table.alias(table_name)
    return table


def get_sql(table, columns, filters=(), aggregations=(), orderings=()):
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
    return query.compile(compile_kwargs={"literal_binds": True})


def get_column_references(columns, filter, aggregations, orderings):
    return columns


def get_table_references(filters):
    return [
        get_table_expression("other_table", None),
        get_table_expression("more_table", None),
    ]


def apply_filter(table_expr, query, query_filter):
    column_name = "date"
    operator = "__ge__"
    value = "2020-02-01"
    column = table_expr.c[column_name]
    method = getattr(column, operator)
    return query.where(method(value))


"""
table = get_table_expression("foo", [])

table2 = get_table_expression("bar", [])
query = sqlalchemy.select([table.c.date])
query = query.select_from(
    sqlalchemy.join(
        query.froms[0], table2, query.froms[0].c.patient_id == table2.c.patient_id
    )
)
query = query.where(table.c.date > table2.c.date)
print(query.compile(compile_kwargs={"literal_binds": True}))
"""
sql = get_sql("sgss", ["patient_id", "positive", "date"], filters=[None, None])
print(sql)
