from cohortextractor.backends.tpp import Backend
from cohortextractor.serialization import (
    cohort_class_to_definition,
    cohort_definition_to_dict,
    cohort_definition_from_dict,
)


def main():
    from study_definition import Cohort

    cohort_definition = cohort_class_to_definition(Cohort)
    # Test roundtrip
    cohort_definition = cohort_definition_from_dict(
        cohort_definition_to_dict(cohort_definition)
    )
    query_engine = Backend.get_query_engine(cohort_definition)
    print(query_engine.get_sql())


if __name__ == "__main__":
    main()
