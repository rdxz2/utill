---
name: lazy-loading
description: Guides for implementing lazy-loaded reusable module objects in Utill.
---

# Lazy-Loading Reusable Module Objects

Use this skill when exposing a module-level reusable object that is initialized on first use, while still allowing direct class instantiation.

Goal:
- Class-based usage: `client = Service(...)`
- Reusable module object: `from utill.module import service`

Implementation rules:
1. Keep the main class public (`Service`) for explicit/custom configuration.
2. Add a private lazy proxy (`_LazyService`) that creates `Service()` only on first attribute access.
3. Use `threading.Lock` for thread-safe initialization.
4. Expose module object with typing: `service: Service = cast(Service, _LazyService())`.
5. Implement `close()` on the proxy to close and reset the cached instance.
6. Avoid variable shadowing with exported object names (for example, use `gcs_client` instead of `gcs` inside functions).

Reference template:

```python
from threading import Lock
from typing import cast


class Service:
    def __init__(self):
        ...

    def close(self):
        ...


class _LazyService:
    def __init__(self):
        self._instance: Service | None = None
        self._lock = Lock()

    def _get_instance(self) -> Service:
        if self._instance is None:
            with self._lock:
                if self._instance is None:
                    self._instance = Service()
        return self._instance

    def __getattr__(self, name: str):
        return getattr(self._get_instance(), name)

    def close(self):
        if self._instance is not None:
            self._instance.close()
            self._instance = None


service: Service = cast(Service, _LazyService())
```

Usage examples:
- `from utill.module import service`
- `service.do_something(...)`
- `from utill.module import Service`
- `service_custom = Service(custom_arg=...)`

Validation checklist:
- Keep imports sorted (Ruff `I` rules).
- Ensure no undefined names.
- Ensure local variables do not shadow exported objects.
