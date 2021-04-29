import textwrap

import sqlalchemy


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
