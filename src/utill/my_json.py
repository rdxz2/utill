import json
import re


def _crawl_dictionary_keys(d: dict, path: tuple = ()) -> list[str]:
    paths: list[tuple] = []

    for key in d.keys():
        key_path = path + (key, )

        # Recursively traverse nested dictionary
        if type(d[key]) is dict:
            result = _crawl_dictionary_keys(d[key], key_path)
        else:
            result = [key_path]

        paths += result  # Combine the array

    return paths


def traverse(data: str | dict) -> list:
    if type(data) == str:
        data = json.loads(data)

    return _crawl_dictionary_keys(data)


def flatten(data: str | dict) -> list:
    if type(data) == str:
        data = json.loads(data)

    return traverse(data)


def get_path(data: dict, path: str) -> str:
    if type(data) != dict:
        raise ValueError('data is not a dictionary!')

    items = path.split('.')
    item = items[0]
    path_remaining = '.'.join(items[1:]) if len(items) > 1 else None

    if item not in data:
        return None

    if path_remaining is None:
        return data[item]

    return get_path(data[item], path_remaining)


def load_jsonc_file(path) -> dict:
    """
    Read a .jsonc (JSON with comment) files, as json.loads cannot read it
    """

    with open(path, 'r') as f:
        content = f.read()
        pattern = r'("(?:\\.|[^"\\])*")|\/\/.*|\/\*[\s\S]*?\*\/'
        content = re.sub(pattern, lambda m: m.group(1) if m.group(1) else '', content)
        return json.loads(content)
