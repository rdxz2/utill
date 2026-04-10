from functools import lru_cache
from importlib import import_module
from typing import Any


@lru_cache(maxsize=None)
def import_module_cached(module_name: str) -> Any:
    return import_module(module_name)


@lru_cache(maxsize=None)
def import_attr_cached(module_name: str, attr_name: str) -> Any:
    return getattr(import_module_cached(module_name), attr_name)
