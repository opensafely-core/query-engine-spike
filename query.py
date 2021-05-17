from dataclasses import dataclass


class frozendict(dict):
    def __hash__(self):
        return id(self)


@dataclass(frozen=True)
class QueryNode:
    name: str
    source: None
    kwargs: frozendict

    def _child(self, name, **kwargs):
        return QueryNode(name=name, source=self, kwargs=frozendict(kwargs))

    def get(self, column):
        return self._child("get", column=column)

    def filter(self, *args, **kwargs):
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
        return self._child("filter", column=args[0], operator=operator, value=value)

    def earliest(self, *args):
        return self.first_by("date")

    def latest(self, *args):
        return self.last_by("date")

    def first_by(self, *columns):
        assert columns
        return self._child("order", direction="desc", columns=columns)

    def last_by(self, *columns):
        assert columns
        return self._child("order", direction="asc", columns=columns)

    def active_as_of(self, date):
        return (
            self.filter("date_start", greater_than_or_equals=date)
            .filter("date_end", less_than_or_equals=date)
            .last_by("date_start", "date_end")
        )


def table(table_name):
    return QueryNode(name="table", source=None, kwargs=frozendict(table_name=table_name))


def codelist(*args, **kwargs):
    pass
