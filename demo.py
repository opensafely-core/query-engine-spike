from collections import defaultdict

import sqlalchemy
import sqlalchemy.dialects.mssql

from cohortextractor.sqlalchemy_utils import (
    make_table_expression,
    get_joined_tables,
    get_primary_table,
)

from query import (
    QueryNode,
    Value,
    ValueFromRow,
    ValueFromAggregate,
    Column,
    BaseTable,
    FilteredTable,
    Row,
)


def main():
    from study_definition import Cohort

    cohort = {
        key: value for key, value in get_class_vars(Cohort) if not key.startswith("_")
    }
    query_engine = QueryEngine(cohort, database_definition=DatabaseDefinition())
    print(query_engine.get_sql())


class DatabaseDefinition:
    def get_table_expression(self, table_name):
        if table_name == "clinical_events":
            return make_table_expression(
                table_name, ["patient_id", "code", "date", "numeric_value"]
            )
        if table_name == "practice_registrations":
            return make_table_expression(
                table_name, ["patient_id", "date_start", "date_end", "stp_code"]
            )
        elif table_name == "sgss_sars_cov_2":
            return self.get_subquery_expression(
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

    def get_subquery_expression(self, table_name, fields, query):
        table = sqlalchemy.text(query)
        table = table.columns(
            sqlalchemy.literal_column("patient_id"),
            *[sqlalchemy.literal_column(field) for field in fields],
        )
        table = table.alias(table_name)
        return table


class QueryEngine:

    sqlalchemy_dialect = sqlalchemy.dialects.mssql

    def __init__(self, column_definitions, database_definition):
        """
        `column_definitions` is a dictionary mapping output column names to
        Values, which are leaf nodes in DAG of QueryNodes

        `database_definition` is a DatabaseDefinition instance
        """
        self.column_definitions = column_definitions
        self.database_definition = database_definition

        # Walk over all nodes in the query DAG looking for output nodes (leaf
        # nodes which represent a value or a column of values) and group them
        # together by "type" and "source" (source being the parent node from
        # which they are derived). Each such group of outputs can be generated
        # by a single query so we want them grouped together.
        output_groups = defaultdict(list)
        for node in self.walk_query_dag(column_definitions.values()):
            if self.is_output_node(node):
                output_groups[self.get_type_and_source(node)].append(node)

        # For each group of output nodes, make a SQLAlchemy table object
        # representing a temporary table into which we will write the required
        # values
        self.temp_tables = {}
        for group, output_nodes in output_groups.items():
            table_name = self.get_new_temporary_table_name()
            columns = {self.get_output_column_name(output) for output in output_nodes}
            self.temp_tables[group] = make_table_expression(
                table_name, {"patient_id"} | columns
            )

        # For each group of output nodes, build a SQLAlchemy query expression
        # to populate the associated temporary table
        self.temp_table_queries = {
            group: self.get_query_expression(output_nodes)
            for group, output_nodes in output_groups.items()
        }

        # `population` is a special-cased boolean column, it doesn't appear
        # itself in the output but it determines what rows are included
        column_definitions = column_definitions.copy()
        population = column_definitions.pop("population")
        is_included, population_table = self.get_value_expression(population)

        # Build big JOIN query which selects the results
        results_query = (
            sqlalchemy.select([population_table.c.patient_id.label("patient_id")])
            .select_from(population_table)
            .where(is_included == True)
        )
        for column_name, output_node in column_definitions.items():
            column, table = self.get_value_expression(output_node)
            results_query = self.include_joined_table(results_query, table)
            results_query = results_query.add_columns(column.label(column_name))

        self.results_query = results_query

    def walk_query_dag(self, nodes):
        parents = []
        for node in nodes:
            yield node
            for attr in ("source", "value"):
                reference = getattr(node, attr, None)
                if isinstance(reference, QueryNode):
                    parents.append(reference)
        if parents:
            yield from self.walk_query_dag(parents)

    @staticmethod
    def is_output_node(node):
        return isinstance(node, (Value, Column))

    def get_type_and_source(self, node):
        assert self.is_output_node(node)
        return (type(node), node.source)

    @staticmethod
    def get_output_column_name(node):
        if isinstance(node, ValueFromAggregate):
            return f"{node.column}_{node.function}"
        elif isinstance(node, (ValueFromRow, Column)):
            return node.column
        else:
            raise TypeError(f"Unhandled type: {node}")

    def get_new_temporary_table_name(self):
        return f"#temp_table_{self.next_counter()}"

    def next_counter(self):
        try:
            self._counter += 1
        except AttributeError:
            self._counter = 1
        return self._counter

    def get_query_expression(self, output_nodes):
        # output_nodes must all be of the same group so we arbitrarily use the
        # first one
        output_type, query_node = self.get_type_and_source(output_nodes[0])

        # Queries (currently) always have a linear structure so we can
        # decompose them into a list
        node_list = self.get_node_list(query_node)
        # The start of the list should always be a BaseTable
        base_table = node_list.pop(0)
        assert isinstance(base_table, BaseTable)
        # If there's an operation applied to reduce the results to a single row
        # per patient, then that will be the final element of the list
        row_selector = None
        if issubclass(output_type, ValueFromRow):
            row_selector = node_list.pop()
            assert isinstance(row_selector, Row)
        # All remaining nodes should be filter operations
        filters = node_list
        assert all(isinstance(f, FilteredTable) for f in filters)

        selected_columns = {node.column for node in output_nodes}
        query = self.get_select_expression(base_table, selected_columns)
        for filter_node in filters:
            query = self.apply_filter(query, filter_node)

        if row_selector is not None:
            query = self.apply_row_selector(
                query,
                sort_columns=row_selector.sort_columns,
                descending=row_selector.descending,
            )

        if issubclass(output_type, ValueFromAggregate):
            query = self.apply_aggregates(query, output_nodes)

        return query

    @staticmethod
    def get_node_list(node):
        node_list = []
        while True:
            node_list.append(node)
            if type(node) is BaseTable:
                break
            else:
                node = node.source
        node_list.reverse()
        return node_list

    def get_select_expression(self, base_table, columns):
        columns = {"patient_id"}.union(columns)
        table_expr = self.database_definition.get_table_expression(base_table.name)
        column_objs = [table_expr.c[column] for column in columns]
        query = sqlalchemy.select(column_objs).select_from(table_expr)
        return query

    def apply_filter(self, query, filter_node):
        column_name = filter_node.column
        operator_name = filter_node.operator
        value_expr, other_table = self.get_value_expression(filter_node.value)
        if other_table is not None:
            query = self.include_joined_table(query, other_table)
        table_expr = get_primary_table(query)
        column = table_expr.c[column_name]
        method = getattr(column, operator_name)
        return query.where(method(value_expr))

    def get_value_expression(self, value):
        if self.is_output_node(value):
            table = self.temp_tables[self.get_type_and_source(value)]
            column = self.get_output_column_name(value)
            value_expr = table.c[column]
            return value_expr, table
        else:
            return value, None

    @staticmethod
    def apply_row_selector(query, sort_columns, descending):
        table_expr = get_primary_table(query)
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

    def apply_aggregates(self, query, aggregate_nodes):
        columns = [
            self.get_aggregate_column(query, aggregate_node)
            for aggregate_node in aggregate_nodes
        ]
        query = query.with_only_columns([query.selected_columns.patient_id] + columns)
        query = query.group_by(query.selected_columns.patient_id)
        return query

    def get_aggregate_column(self, query, aggregate_node):
        output_column = self.get_output_column_name(aggregate_node)
        if aggregate_node.function == "exists":
            return sqlalchemy.literal(True).label(output_column)
        else:
            function = getattr(sqlalchemy.func, aggregate_node.function)
            source_column = aggregate_node.column
            return function(query.selected_columns[source_column]).label(output_column)

    @staticmethod
    def include_joined_table(query, table):
        if table.name in [t.name for t in get_joined_tables(query)]:
            return query
        join = sqlalchemy.join(
            query.froms[0],
            table,
            query.selected_columns.patient_id == table.c.patient_id,
            isouter=True,
        )
        return query.select_from(join)

    def get_sql(self):
        sql = []
        for group, table in self.temp_tables.items():
            query = self.temp_table_queries[group]
            query_sql = self.query_expression_to_sql(query)
            sql.append(f"SELECT * INTO {table.name} FROM (\n{query_sql}\n) t")
        sql.append(self.query_expression_to_sql(self.results_query))

        return "\n\n\n".join(sql)

    def query_expression_to_sql(self, query):
        return str(
            query.compile(
                dialect=self.sqlalchemy_dialect.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )


def get_class_vars(cls):
    default_vars = set(dir(type("ArbitraryEmptyClass", (), {})))
    return [(key, value) for key, value in vars(cls).items() if key not in default_vars]


if __name__ == "__main__":
    main()
