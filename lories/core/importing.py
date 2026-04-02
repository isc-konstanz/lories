# -*- coding: utf-8 -*-
"""
lories.core.importing
~~~~~~~~~~~~~~~~~~~~~

Utilities for loading modules that have optional third-party dependencies.

Usage in a package ``__init__.py``::

    from lories.core.importing import inject_mock

    for _mod in ["cameras", "modbus", ...]:
        _attempts = 0
        while True:
            try:
                importlib.import_module(f".{_mod}", __name__)
                break
            except ImportError as e:
                missing = e.name or str(e).split("'")[1]
                if _attempts > 0:
                    _unavailable[_mod] = missing
                    break
                inject_mock(missing)
                sys.modules.pop(f"{__name__}.{_mod}", None)
                _attempts += 1

"""

import sys
from types import ModuleType


class MockModule(ModuleType):
    """Stands in for any missing third-party dependency.

    Attribute access always succeeds so the importing module loads cleanly.
    Instantiation / calls raise a clear ``RuntimeError`` at runtime.
    """

    def __init__(self, name: str):
        super().__init__(name)
        self._mock_name = name

    def __getattr__(self, item: str):
        # Let Python handle module-protocol dunders normally (werkzeug reads __file__, etc.)
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        cls = type(
            item,
            (),
            {
                "__module__": self._mock_name,
                "__doc__": f"Unavailable — `{self._mock_name}` is not installed.",
                "__init__": lambda self_, *a, **kw: (_ for _ in ()).throw(
                    RuntimeError(f"'{self._mock_name}' is not installed. " f"pip install {self._mock_name}")
                ),
            },
        )
        setattr(self, item, cls)
        return cls

    def __repr__(self):
        return f"<MockModule '{self._mock_name}'>"


def inject_mock(missing_dep: str) -> None:
    """Register a :class:`MockModule` for *missing_dep* in ``sys.modules``.

    Also covers any submodule entries that Python may have cached as ``None``
    after a failed partial import (e.g. ``pymodbus.client``).
    """
    top = missing_dep.split(".")[0]
    if top not in sys.modules:
        sys.modules[top] = MockModule(top)
    for key in list(sys.modules):
        if key == top or key.startswith(top + "."):
            if sys.modules[key] is None:
                sys.modules[key] = MockModule(key)
