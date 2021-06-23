from cohortextractor.query_engines import mssql

from .base import BackendBase, Table, Column


class Backend(BackendBase):

    query_engine_class = mssql.QueryEngine

    clinical_events = Table(
        source="CodedEvents",
        columns=dict(
            code=Column("code", system="ctv3", source="CTV3Code"),
            date=Column("datetime", source="ConsultationDate"),
            numeric_value=Column("float", source="NumericValue"),
        ),
    )

    sgss_sars_cov_2 = Table(
        columns=dict(
            date=Column("date"),
            positive_result=Column("boolean"),
        )
    )

    @sgss_sars_cov_2.query
    def sgss_sars_cov_2_query():
        return """
        SELECT patient_id, date, True AS positive_result FROM sgss_positive
        UNION ALL
        SELECT patient_id, date, False AS positive_result FROM sgss_negative
        """

    practice_registrations = Table(
        source="RegistrationHistory",
        columns=dict(
            date_start=Column("date", source="StartDate"),
            date_end=Column("date", source="EndDate"),
            stp_code=Column("categorical", source="STPCode"),
        ),
    )
