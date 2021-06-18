import sqlalchemy


def make_table_expression(table_name, columns):
    return sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        *[sqlalchemy.Column(column) for column in columns],
    )


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
