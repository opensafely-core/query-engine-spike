import sqlalchemy


class BackendBase:
    def __init__(self):
        # Quick and dirty way to make sure each table and column knows what
        # name it was assigned to
        for key, value in vars(self.__class__).items():
            if isinstance(value, Table):
                value.name = key
                for column_name, column_def in value.columns.items():
                    column_def.name = column_name

    @classmethod
    def get_query_engine(cls, column_definitions):
        return cls.query_engine_class(column_definitions, backend=cls())

    def get_table_expression(self, table_name):
        table = getattr(self, table_name, None)
        if not isinstance(table, Table):
            raise ValueError(f"Unknown table '{table_name}'")
        table_expression = table.get_query()
        table_expression = table_expression.alias(table_name)
        return table_expression


class Table:

    query_function = None

    def __init__(self, *, columns, source=None):
        if "patient_id" not in columns:
            columns["patient_id"] = Column("int")
        self.source = source
        self.columns = columns

    def get_column_names(self):
        return self.columns.keys()

    def query(self, query_function):
        # Decorator to register a function as the query function
        self.query_function = query_function
        return query_function

    def get_query(self):
        if self.query_function:
            query = sqlalchemy.text(self.query_function())
            query = query.columns(
                *[
                    sqlalchemy.literal_column(column)
                    for column in self.get_column_names()
                ]
            )
            return query
        else:
            return self.default_query_function()

    def default_query_function(self):
        table_name = self.source or self.name
        columns = []
        for name, column in self.columns.items():
            source = column.source or column.name
            columns.append(sqlalchemy.literal_column(source).label(name))
        query = sqlalchemy.select(columns).select_from(sqlalchemy.table(table_name))
        return query


class Column:
    def __init__(self, column_type, source=None, system=None):
        self.type = column_type
        self.source = source
        self.system = system
