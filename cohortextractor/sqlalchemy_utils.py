import sqlalchemy


def make_table_expression(table_name, columns):
    """
    Return a SQLAlchemy object representing a table with the given name and
    columns
    """
    return sqlalchemy.Table(
        table_name,
        sqlalchemy.MetaData(),
        *[sqlalchemy.Column(column) for column in columns],
    )


def get_joined_tables(query):
    """
    Given a query object return a list of all tables referenced
    """
    tables = []
    from_exprs = list(query.froms)
    while from_exprs:
        next_expr = from_exprs.pop()
        if isinstance(next_expr, sqlalchemy.sql.selectable.Join):
            from_exprs.extend([next_expr.left, next_expr.right])
        else:
            tables.append(next_expr)
    # The above algorithm produces tables in right to left order, but it makes
    # more sense to return them as left to right
    tables.reverse()
    return tables


def get_primary_table(query):
    """
    Return the left-most table referenced in the query
    """
    return get_joined_tables(query)[0]
