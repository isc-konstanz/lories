#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
lories
~~~~~~

To learn how to use local resource integration systems, see "lories --help"

"""

import os
from argparse import ArgumentParser, RawTextHelpFormatter

os.environ["NUMEXPR_MAX_THREADS"] = str(os.cpu_count())


def main() -> None:
    import lories

    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {lories.__version__}")

    application = lories.load(parser=parser)
    application.main()


if __name__ == "__main__":
    main()
