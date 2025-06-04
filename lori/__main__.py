#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
lori
~~~~

To learn how to use local resource integration systems, see "lori --help"

"""

import os
from argparse import ArgumentParser, RawTextHelpFormatter

os.environ["NUMEXPR_MAX_THREADS"] = str(os.cpu_count())


def main() -> None:
    import lori

    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {lori.__version__}")

    application = lori.load(parser=parser)
    application.main()


if __name__ == "__main__":
    main()
