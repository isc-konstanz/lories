# -*- coding: utf-8 -*-
"""
    loris.settings
    ~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Dict, Any

import os
import sys
import shutil

import logging.config

from argparse import ArgumentParser
from loris.configs import Configurations, Directories


class Settings(Configurations):

    app: str

    # noinspection PyProtectedMember, SpellCheckingInspection
    def __init__(self,
                 application: str,
                 conf_file: str = 'settings.conf',
                 parser: ArgumentParser = None) -> None:
        kwargs = _parse_kwargs(parser)

        conf_dirs = Directories(**{d: kwargs.pop(d, None) for d in Directories.KEYS})
        conf_path = os.path.join(conf_dirs.conf, conf_file)

        if os.path.isfile(conf_path):
            kwargs.update(self._load(conf_path))

        super().__init__(conf_file, conf_path, conf_dirs, **kwargs)
        self.application = application

        self.dirs.join(self)
        if self.dirs._conf is None:
            self.dirs._conf = os.path.dirname(self.path)

        logging_file = os.path.join(self.dirs.conf, 'logging.conf')
        if not os.path.isfile(logging_file):
            logging_default = logging_file.replace('logging.conf', 'logging.default.conf')
            if os.path.isfile(logging_default):
                shutil.copy(logging_default, logging_file)
        if os.path.isfile(logging_file):
            logging.config.fileConfig(logging_file)
            for handler in logging.getLoggerClass().root.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.dirs._log = os.path.dirname(handler.baseFilename)
                    break
        else:
            handler_console = logging.StreamHandler(sys.stdout)
            handler_console.setLevel(logging.INFO)
            handler_console.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d - %(name)s - %(message)s",
                                                           "%Y-%m-%d %H:%M:%S"))

            # TODO: Think about fallback logging configuration necessity to use default logfile location
            # log_file = application.lower()
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
        if not os.path.isdir(self.dirs.log):
            os.makedirs(self.dirs.log, exist_ok=True)

        override_path = os.path.join(self.dirs.data, self.name)
        if os.path.isfile(override_path):
            from configs import load_toml
            self.update(load_toml(override_path))
            self.dirs.join(self)

    # def __getattr__(self, attr):
    #     if self.has_option(self.GENERAL, attr):
    #         return self.get(self.GENERAL, attr)
    #     try:
    #         # noinspection PyUnresolvedReferences
    #         return super().__getattr__(attr)
    #
    #     except AttributeError:
    #         raise AttributeError("'{0}' object has no attribute '{1}'".format(type(self).__name__, attr))


def _parse_kwargs(parser: ArgumentParser) -> Dict[str, Any]:
    if parser is not None:
        parser.add_argument('-c', '--conf-dir',
                            dest='conf_dir',
                            metavar='dir',
                            help="directory to expect root configuration files")

        parser.add_argument('-l', '--lib-dir',
                            dest='lib_dir',
                            metavar='dir',
                            help="directory to expect and write library files to")

        parser.add_argument('-d', '--data-dir',
                            dest='data_dir',
                            metavar='dir',
                            help="directory to expect and write data files to")

        parser.add_argument('--system-scan',
                            dest='system_scan',
                            action='store_true',
                            help="flags whether several systems will be expected, instead of a single one")

        parser.add_argument('--system-flat',
                            dest='system_flat',
                            action='store_true',
                            help="flags if the configuration files will be expected directly in the data directory,"
                                 "instead of a corresponding 'conf' directory")

        parser.add_argument('--system-copy',
                            dest='system_copy',
                            action='store_true',
                            help="flags if the configured system files should be "
                                 "copied to the specified data directory if it is empty")

        args = parser.parse_args()
        kwargs = vars(args)
    else:
        kwargs = {}

    return kwargs
