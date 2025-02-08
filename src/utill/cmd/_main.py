def _unique(strings: list[str], sort: bool = True):
    [print(x) for x in (sorted(set(strings)) if sort else set(strings))]
