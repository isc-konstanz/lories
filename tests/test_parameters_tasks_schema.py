# -*- coding: utf-8 -*-
"""
lories.tests.test_parameters_tasks_schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Descriptor resolution for the task-executor ``[tasks]`` group on
``TaskContext`` and the ``create`` flag on the SQL ``Schema``.
"""

from __future__ import annotations

from lories.connectors.sql.schema import Schema
from lories.data.tasks import TaskContext


class _PlainTaskContext(TaskContext):
    """Minimal concrete TaskContext; these tests only exercise the executor build."""

    @property
    def processors(self):
        return None

    @property
    def converters(self):
        return None

    @property
    def connectors(self):
        return None

    def _filter_connectors(self, *filters):
        return []


def _executor(ctx: TaskContext):
    return getattr(ctx, "_TaskContext__executor")


def test_tasks_workers_max_resolved(write_conf):
    configs = write_conf("[tasks]\nworkers_max = 3\n")
    ctx = _PlainTaskContext(configs=configs)
    ctx.configure(configs)
    try:
        assert ctx._tasks_section == {"workers_max": 3}
        assert _executor(ctx)._max_workers == 3
    finally:
        _executor(ctx).shutdown(wait=False)


def test_tasks_workers_max_defaults_without_section(write_conf):
    configs = write_conf("enabled = true\n")
    ctx = _PlainTaskContext(configs=configs)
    ctx.configure(configs)
    try:
        assert ctx._tasks_section is None
        assert _executor(ctx)._max_workers >= 1
    finally:
        _executor(ctx).shutdown(wait=False)


def test_schema_create_defaults_true(write_conf):
    schema = Schema(dialect=None)
    schema.configure(write_conf("enabled = true\n"))
    assert schema._create is True


def test_schema_create_explicit_false(write_conf):
    schema = Schema(dialect=None)
    schema.configure(write_conf("create = false\n"))
    assert schema._create is False
