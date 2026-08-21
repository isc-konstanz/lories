# -*- coding: utf-8 -*-
"""
lories.tests.test_application_settings_logging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regression: ``Settings._load_logging`` applies site logging configs via
``logging.config.fileConfig``, whose default ``disable_existing_loggers=True``
hard-disables every logger already created at import time. Site configs
declare only ``keys=root``, so whether a module logger survived depended on
import order -- in production this silently muted component loggers and hid
stall warnings. A pre-existing logger must stay enabled and its records must
still propagate to the root handlers after the config is applied.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

from lories.application.settings import Settings

_ROOT_ONLY_LOGGING_CONF = """
[loggers]
keys=root

[handlers]
keys=console

[formatters]
keys=simple

[logger_root]
level=INFO
handlers=console

[handler_console]
class=StreamHandler
level=INFO
formatter=simple
args=(sys.stdout,)

[formatter_simple]
format=%(name)s - %(message)s
"""


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_existing_logger_survives_root_only_logging_conf(tmp_path):
    conf_dir = tmp_path / "conf"
    conf_dir.mkdir()
    (conf_dir / "logging.conf").write_text(_ROOT_ONLY_LOGGING_CONF)

    # Created BEFORE the config is applied, like any module-level logger of an
    # already-imported component.
    logger = logging.getLogger("tests.settings_logging.preexisting")
    logger.disabled = False

    settings = Settings.__new__(Settings)
    # `dirs` is a read-only property over the name-mangled Configurations attribute.
    settings._Configurations__dirs = SimpleNamespace(conf=str(conf_dir), log=str(tmp_path / "log"))
    Settings._load_logging(settings)

    assert logger.disabled is False, "fileConfig must not hard-disable already-created loggers"

    handler = _ListHandler()
    logging.getLogger().addHandler(handler)
    try:
        logger.info("still alive")
    finally:
        logging.getLogger().removeHandler(handler)
    assert any(record.getMessage() == "still alive" for record in handler.records)
