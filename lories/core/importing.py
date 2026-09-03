# -*- coding: utf-8 -*-
"""
lories.core.importing
~~~~~~~~~~~~~~~~~~~~~

Utilities for loading modules that have optional third-party dependencies.

"""

import importlib.abc
import importlib.util
import sys
from types import ModuleType


class _MockMeta(type):
    """Metaclass of fabricated classes: attribute access on them fabricates further
    classes (``io.RISING``-style module-level constants), so the importing module
    still loads cleanly. Dunder lookups keep raising, as probes expect."""

    def __getattr__(cls, item: str):
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        value = _mock_class(cls.__module__, item)
        setattr(cls, item, value)
        return value


def _mock_class(mock_name: str, item: str) -> type:
    return _MockMeta(
        item,
        (),
        {
            "__module__": mock_name,
            "__doc__": f"Unavailable — `{mock_name}` is not installed.",
            "__init__": lambda self, *a, **kw: (_ for _ in ()).throw(
                RuntimeError(f"'{mock_name}' is not installed. pip install {mock_name}")
            ),
        },
    )


class MockModule(ModuleType):
    """Stands in for any missing third-party dependency.

    Attribute access always succeeds so the importing module loads cleanly.
    Instantiation / calls raise a clear ``RuntimeError`` at runtime.
    """

    def __init__(self, name: str):
        super().__init__(name)
        self._mock_name = name
        # Present as a package, so submodule imports reach the meta-path finder
        # instead of failing with "'<name>' is not a package".
        self.__path__: list = []

    def __getattr__(self, item: str):
        if item.startswith("__") and item.endswith("__"):
            raise AttributeError(item)
        cls = _mock_class(self._mock_name, item)
        setattr(self, item, cls)
        return cls

    def __repr__(self):
        return f"<MockModule '{self._mock_name}'>"


class _MockLoader(importlib.abc.Loader):
    def create_module(self, spec):
        return MockModule(spec.name)

    def exec_module(self, module):
        pass


class _MockFinder(importlib.abc.MetaPathFinder):
    """Serves a :class:`MockModule` for any name at or below a mocked prefix.

    Appended at the END of ``sys.meta_path``: the real finders get the first
    shot, so a partially importable real package keeps winning where it can and
    the mock only fills the genuinely unimportable names.
    """

    def __init__(self):
        self.prefixes: set[str] = set()

    def covers(self, name: str) -> bool:
        return any(name == prefix or name.startswith(prefix + ".") for prefix in self.prefixes)

    def find_spec(self, name, path=None, target=None):
        if not self.covers(name):
            return None
        return importlib.util.spec_from_loader(name, _MockLoader(), is_package=True)


_finder = _MockFinder()


def is_mocked(name: str) -> bool:
    """True when *name* falls under a prefix already mocked by :func:`inject_mock`."""
    return _finder.covers(name)


def inject_mock(missing_dep: str) -> None:
    """Mock *missing_dep* — and everything below it — so importing modules load cleanly.

    The top-level package is registered as a :class:`MockModule` in
    ``sys.modules``; submodule imports (``from pkg.sub import X``) are served by
    a meta-path finder, so the importing module may use any import style. Also
    covers submodule entries that Python may have cached as ``None`` after a
    failed partial import (e.g. ``pymodbus.client``).
    """
    if _finder not in sys.meta_path:
        sys.meta_path.append(_finder)
    _finder.prefixes.add(missing_dep)

    top = missing_dep.split(".")[0]
    if top not in sys.modules:
        sys.modules[top] = MockModule(top)
    for key in list(sys.modules):
        if key == top or key.startswith(top + "."):
            if sys.modules[key] is None:
                sys.modules[key] = MockModule(key)
