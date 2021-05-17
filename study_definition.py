from query import table, codelist

creatinine_codes = codelist(["XE2q5"], system="ctv3")


class Cohort:
    sgss_first_positive_test_date = (
        table("sgss_sars_cov_2").filter(positive_result=True).earliest().get("date")
    )

    _creatinine = (
        table("clinical_events")
        .filter(code=creatinine_codes)
        .filter("date", between=["2015-03-01", sgss_first_positive_test_date])
        .latest()
    )
    creatinine_value = _creatinine.get("numeric_value")
    creatinine_date = _creatinine.get("date")

    stp = (
        table("practice_registrations")
        .active_as_of(sgss_first_positive_test_date)
        .get("stp_code")
    )
