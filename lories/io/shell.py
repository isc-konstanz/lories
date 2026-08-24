# -*- coding: utf-8 -*-
"""
lories.io.shell
~~~~~~~~~~~~~~~

Terminal / shell utilities.

"""

import os
import sys


def supports_color() -> bool:
    """Return *True* when the current stderr can render ANSI escape codes.

    Checks the actual TTY flag as well as well-known IDE and terminal
    environment variables so that coloured output works in PyCharm, VS Code,
    and most modern terminal emulators even when they are not a real PTY.
    """
    if hasattr(sys.stderr, "isatty") and sys.stderr.isatty():
        return True
    if os.environ.get("PYCHARM_HOSTED"):
        return True
    if os.environ.get("TERM_PROGRAM") == "vscode":
        return True
    if os.environ.get("COLORTERM"):
        return True
    return False


#: ANSI bold-yellow — begin highlight (empty string when colour is unsupported).
ANSI_WARN = "\033[1;33m" if supports_color() else ""
#: ANSI reset — end highlight.
ANSI_RESET = "\033[0m" if supports_color() else ""
#: ANSI dim-yellow — key name inside a warning.
ANSI_KEY = "\033[33m" if supports_color() else ""
