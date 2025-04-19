def _random(length: int, alphanum: bool):
    from ..my_string import generate_random_string

    print(generate_random_string(length, alphanum))


def _unique(strings: list[str], sort: bool = True):
    [print(x) for x in (sorted(set(strings)) if sort else set(strings))]
