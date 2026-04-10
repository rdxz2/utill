from typing import Any
from typing import cast

from ._lazy_import import import_attr_cached


class _LazyLogger:
    def __getattr__(self, name: str) -> Any:
        return getattr(import_attr_cached("loguru", "logger"), name)


logger = cast(Any, _LazyLogger())
