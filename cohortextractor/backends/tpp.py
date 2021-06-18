import sqlalchemy

from cohortextractor.query_engines import mssql
from cohortextractor.sqlalchemy_utils import make_table_expression


class Backend:

    query_engine_class = mssql.QueryEngine

    @classmethod
    def get_query_engine(cls, column_definitions):
        return cls.query_engine_class(column_definitions, backend=cls())

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
