from query import table, codelist

creatinine_codes = codelist(["XE2q5"], system="ctv3")


class Cohort:
    _sgss_positives = table("sgss_sars_cov_2").filter(positive_result=True)
    sgss_first_positive_test_date = _sgss_positives.earliest().get("date")
    sgss_last_positive_test_date = _sgss_positives.latest().get("date")

    _creatinine = (
        table("clinical_events")
        .filter(code=creatinine_codes)
        .filter(
            "date",
            between=[sgss_first_positive_test_date, sgss_last_positive_test_date],
        )
        .filter(
            "date",
            between=[sgss_first_positive_test_date, sgss_last_positive_test_date],
        )
        .latest()
    )
    creatinine_value = _creatinine.get("numeric_value")
    creatinine_date = _creatinine.get("date")

    stp = (
        table("practice_registrations")
        .active_as_of(sgss_first_positive_test_date)
        .get("stp_code")
    )
