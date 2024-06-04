#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    loris
    ~~~~~

    To learn how to use local resource integration systems, see "loris --help"

"""
import os
from argparse import ArgumentParser, RawTextHelpFormatter

import loris

os.environ["NUMEXPR_MAX_THREADS"] = str(os.cpu_count())


def main() -> None:
    with loris.load(parser=_get_parser()) as application:
        if application.settings["action"] == "run":
            application.run()
        elif application.settings["action"] == "start":
            application.start()
            application.wait()


def _get_parser() -> ArgumentParser:
    from loris import __version__

    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument("-v", "--version", action="version", version="%(prog)s {version}".format(version=__version__))

    subparsers = parser.add_subparsers(dest="action")
    # subparsers.required = True
    subparsers.default = "run"
    subparsers.add_parser("run", help="run local resources, connectors and systems")
    subparsers.add_parser("start", help="start the local resource system")

    return parser


if __name__ == "__main__":
    main()
