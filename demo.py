from cohortextractor.backends.tpp import Backend


def main():
    from study_definition import Cohort

    cohort = {
        key: value for key, value in get_class_vars(Cohort) if not key.startswith("_")
    }
    query_engine = Backend.get_query_engine(cohort)
    print(query_engine.get_sql())


def get_class_vars(cls):
    default_vars = set(dir(type("ArbitraryEmptyClass", (), {})))
    return [(key, value) for key, value in vars(cls).items() if key not in default_vars]


if __name__ == "__main__":
    main()
