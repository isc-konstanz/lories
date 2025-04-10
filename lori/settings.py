# -*- coding: utf-8 -*-
"""
lori.settings
~~~~~~~~~~~~~


"""

from __future__ import annotations

import logging.config
import os
import shutil
import sys
from argparse import ArgumentParser
from typing import Any, Dict

from lori.core import Configurations, Directories, Directory
from lori.util import to_bool


class Settings(Configurations):
    app: str

    # noinspection PyProtectedMember, SpellCheckingInspection
    def __init__(self, app_name: str, app_file: str = "settings.conf", parser: ArgumentParser = None) -> None:
        app_args = _parse_kwargs(parser)
        app_args["name"] = app_name
        app_args["systems"] = {k: to_bool(app_args.pop(f"system_{k}", False)) for k in ["scan", "flat", "copy"]}
        app_dirs = Directories(**{d: app_args.pop(d, None) for d in Directories.KEYS})
        super().__init__(app_file, app_dirs, app_args)
        self._load(require=False)
        self._load_logging()

        override_path = os.path.join(self.dirs.data, self.name)
        if os.path.isfile(override_path):
            self._load_toml(override_path)
            self.dirs.update(self.get_section(Directories.SECTION, defaults={}))
        if self.dirs.conf._dir is None:
            self.dirs._conf = Directory(os.path.dirname(self.path), default="conf")

    def __getattr__(self, attr):
        # __getattr__ gets called when the item is not found via __getattribute__
        # To avoid recursion, call __getattribute__ directly to get components dict
        configs = Configurations.__getattribute__(self, f"_{Configurations.__name__}__configs")
        if attr in configs.keys():
            return configs[attr]
        raise AttributeError(f"'{type(self).__name__}' object has no configuration '{attr}'")

    @property
    def _sections_dir(self) -> Directory:
        return self.dirs.conf

    def _load_logging(self) -> None:
        logging_file = os.path.join(self.dirs.conf, "logging.conf")
        if not os.path.isfile(logging_file) and not self.dirs.conf.is_default():
            logging_default = logging_file.replace("logging.conf", "logging.default.conf")
            if os.path.isfile(logging_default):
                shutil.copy(logging_default, logging_file)

        if os.path.isfile(logging_file):
            logging.config.fileConfig(logging_file)
            for handler in logging.getLoggerClass().root.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.dirs.log = os.path.dirname(handler.baseFilename)
                    break
            if not os.path.isdir(self.dirs.log):
                os.makedirs(self.dirs.log, exist_ok=True)
        else:
            handler_console = logging.StreamHandler(sys.stdout)
            handler_console.setLevel(logging.INFO)
            handler_console.setFormatter(
                logging.Formatter("%(asctime)s.%(msecs)03d - %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S")
            )

            # TODO: Think about fallback logging configuration necessity to use default logfile location
            # log_file = app_name.lower()
            # if not log_file.endswith('.log'):
            #     log_file += '.log'
            # handler_file = logging.FileHandler(os.path.join(self.dirs._log or 'log', log_file))
            # handler_file.setLevel(logging.WARN)
            # handler_file.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s",
            #                                             "%Y-%m-%d %H:%M:%S"))
            logging.basicConfig(
                force=True,
                level=logging.INFO,
                handlers=[
                    handler_console,
                    # handler_file,
                ],
            )


# noinspection PyProtectedMember
def _parse_kwargs(parser: ArgumentParser) -> Dict[str, Any]:
    if parser is not None:
        action_parsers = [a for a in parser._actions if a.dest == "action"]
        if len(action_parsers) > 0:
            action_parser = next(iter(action_parsers))
        else:
            action_parser = parser.add_subparsers(dest="action")
        if not action_parser.required and action_parser.default is None:
            action_parser.required = True
        if "run" not in action_parser.choices.keys():
            action_parser.add_parser("run", help="run local resources, connectors and systems")
        if "start" not in action_parser.choices.keys():
            action_parser.add_parser("start", help="start the local resource system")
        if "rotate" not in action_parser.choices.keys():
            replicate_parser = action_parser.add_parser(
                name="rotate",
                help="rotate data of a local database",
            )
            replicate_parser.add_argument(
                "--full",
                dest="full",
                action="store_true",
                help="flags if the rotation should be executed for all data",
            )
        if "replicate" not in action_parser.choices.keys():
            replicate_parser = action_parser.add_parser(
                name="replicate",
                help="replicate data from a remote database",
            )
            replicate_parser.add_argument(
                "--force",
                dest="force",
                action="store_true",
                help="flags if the replication should force rewrite data for checksum mismatches",
            )
            replicate_parser.add_argument(
                "--full",
                dest="full",
                action="store_true",
                help="flags if the replication should be executed for all data",
            )
        if "simulate" not in action_parser.choices.keys():
            simulate_parser = action_parser.add_parser(
                name="simulate",
                help="simulate local resources and systems for a specified time range",
            )
            simulate_parser.add_argument(
                "--start",
                dest="start",
                metavar="datetime",
                help="the start datetime for the simulation to start",
            )
            simulate_parser.add_argument(
                "--end",
                dest="end",
                metavar="datetime",
                help="the end datetime for the simulation to end",
            )

        parser.add_argument(
            "-c",
            "--conf-dir",
            dest="conf_dir",
            metavar="dir",
            help="directory to expect root configuration files",
        )
        parser.add_argument(
            "-d",
            "--data-dir",
            dest="data_dir",
            metavar="dir",
            help="directory to expect and write data files to",
        )
        parser.add_argument(
            "--system-scan",
            dest="system_scan",
            action="store_true",
            help="flags whether several systems will be expected, instead of a single one",
        )
        parser.add_argument(
            "--system-flat",
            dest="system_flat",
            action="store_true",
            help="flags if the configuration files will be expected directly in the data directory, "
            + "instead of a corresponding 'conf' directory",
        )
        parser.add_argument(
            "--system-copy",
            dest="system_copy",
            action="store_true",
            help="flags if the configured system files should be copied to the specified data directory if it is empty",
        )

        args = parser.parse_args()
        kwargs = vars(args)
    else:
        kwargs = {}

    return kwargs
