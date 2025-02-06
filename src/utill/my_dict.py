class AutoPopulatingDict(dict):
    def __init__(self, fetch_function, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_function = fetch_function

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            value = self.fetch_function(key)
            self[key] = value
            return value
