def table(table_name):
    return BaseTable(table_name)


def codelist(*args, **kwargs):
    pass


class QueryNode:
    pass


class BaseTable(QueryNode):
    def __init__(self, name):
        self.name = name

    def get(self, column):
        return Column(source=self, column=column)

    def filter(self, *args, **kwargs):
        # Syntactic suger
        if not args:
            assert kwargs
            node = self
            for field, value in kwargs.items():
                node = node.filter(field, equals=value)
            return node
        elif len(kwargs) > 1:
            node = self
            for operator, value in kwargs.items():
                node = node.filter(*args, **{operator: value})
            return node
        assert len(args) == 1
        operator, value = list(kwargs.items())[0]
        if operator == "between":
            return self.filter(*args, on_or_after=value[0], on_or_before=value[1])

        translations = {
            "equals": "__eq__",
            "less_than": "__lt__",
            "less_than_or_equals": "__le__",
            "greater_than": "__gt__",
            "greater_than_or_equals": "__ge__",
            "on_or_before": "__le__",
            "on_or_after": "__ge__",
        }
        operator = translations[operator]
        return FilteredTable(
            source=self, column=args[0], operator=operator, value=value
        )

    def earliest(self, *args):
        return self.first_by("date")

    def latest(self, *args):
        return self.last_by("date")

    def first_by(self, *columns):
        assert columns
        return RowFromOrderBy(source=self, sort_columns=columns, descending=True)

    def last_by(self, *columns):
        assert columns
        return RowFromOrderBy(source=self, sort_columns=columns, descending=False)

    def active_as_of(self, date):
        return (
            self.filter("date_start", greater_than_or_equals=date)
            .filter("date_end", less_than_or_equals=date)
            .last_by("date_start", "date_end")
        )

    def count(self):
        return self.aggregate("COUNT", "*")

    def aggregate(self, function, column):
        return RowFromAggregate(self, function, column)


class FilteredTable(BaseTable):
    def __init__(self, source, column, operator, value):
        self.source = source
        self.column = column
        self.operator = operator
        self.value = value


class Row(QueryNode):
    def get(self, column):
        return Value(source=self, column=column)


class RowFromOrderBy(Row):
    def __init__(self, source, sort_columns, descending=False):
        self.source = source
        self.sort_columns = sort_columns
        self.descending = descending


class RowFromAggregate(Row):
    def __init__(self, source, function, column):
        self.source = source
        self.function = function
        self.column = column


class Column(QueryNode):
    def __init__(self, source, column):
        self.source = source
        self.column = column


class Value(QueryNode):
    def __init__(self, source, column):
        self.source = source
        self.column = column
