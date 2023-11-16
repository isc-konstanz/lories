# -*- coding: utf-8 -*-
"""
    corsys.settings
    ~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from typing import Dict, Any

import os
import sys
import shutil
from argparse import ArgumentParser
from .configs import Configurations, Directories


class Settings(Configurations):

    # noinspection PyProtectedMember, SpellCheckingInspection
    def __init__(self,
                 name: str,
                 conf_file='settings.cfg',
                 parser: ArgumentParser = None) -> None:
        super().__init__(conf_file, require=False, **_parse_kwargs(parser))

        logging_file = os.path.join(self.dirs._conf or 'conf', 'logging.cfg')
        if not os.path.isfile(logging_file):
            logging_default = logging_file.replace('logging.cfg', 'logging.default.cfg')
            if os.path.isfile(logging_default):
                shutil.copy(logging_default, logging_file)

        # Load the logging configuration
        import logging.config

        if os.path.isfile(logging_file):
            logging.config.fileConfig(logging_file)
            for handler in logging.getLoggerClass().root.handlers:
                if isinstance(handler, logging.FileHandler):
                    self.dirs._log = os.path.dirname(handler.baseFilename)
        else:
            handler_console = logging.StreamHandler(sys.stdout)
            handler_console.setLevel(logging.INFO)
            handler_console.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d - %(name)s - %(message)s",
                                                           "%Y-%m-%d %H:%M:%S"))

            handler_file = logging.FileHandler(os.path.join(self.dirs._log or 'log', f'{name}.log'))
            handler_file.setLevel(logging.WARN)
            handler_file.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s",
                                                        "%Y-%m-%d %H:%M:%S"))

            logging.basicConfig(
                level=logging.INFO,
                handlers=[
                    handler_console,
                    handler_file,
                ],
            )
        if not os.path.isdir(self.dirs.log):
            os.makedirs(self.dirs.log, exist_ok=True)

        override_path = os.path.join(self.dirs.data, conf_file)
        if os.path.isfile(override_path):
            self.read(override_path, encoding='utf-8')
            self.dirs.join(self)

    @property
    def general(self) -> Dict[str, str]:
        return dict({k: v for k, v in self.items(self.GENERAL)
                     if k not in Directories.KEYS + ['system_scan', 'system_copy']})

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
        parser.add_argument('-c', '--conf-directory',
                            dest='conf_dir',
                            help="directory to expect basic configuration files",
                            metavar='DIR')

        parser.add_argument('-l', '--lib-directory',
                            dest='lib_dir',
                            help="directory to expect and write library files to",
                            metavar='DIR')

        parser.add_argument('-d', '--data-directory',
                            dest='data_dir',
                            help="directory to expect and write result files to",
                            metavar='DIR')

        args = parser.parse_args()
        kwargs = vars(args)
    else:
        kwargs = {}

    return kwargs
