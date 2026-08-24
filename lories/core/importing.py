# -*- coding: utf-8 -*-
"""
lories.core.importing
~~~~~~~~~~~~~~~~~~~~~

Utilities for loading modules that have optional third-party dependencies.

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
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        cls = type(
            item,
            (),
            {
                "__module__": self._mock_name,
                "__doc__": f"Unavailable — `{self._mock_name}` is not installed.",
                "__init__": lambda self_, *a, **kw: (_ for _ in ()).throw(
                    RuntimeError(f"'{self._mock_name}' is not installed. pip install {self._mock_name}")
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
