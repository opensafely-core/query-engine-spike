import json

from cohortextractor.serialization import (
    cohort_class_to_definition,
    cohort_definition_to_dict,
)


def main():
    from study_definition import Cohort

    cohort_definition = cohort_class_to_definition(Cohort)
    data = cohort_definition_to_dict(cohort_definition)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
