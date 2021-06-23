from cohortextractor.serialization import serialize_cohort_definition


def main():
    from study_definition import Cohort

    print(serialize_cohort_definition(Cohort))


if __name__ == "__main__":
    main()
