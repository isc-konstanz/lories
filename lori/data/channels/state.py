# -*- coding: utf-8 -*-
"""
lori.data.channels.state
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from enum import Enum


class ChannelState(Enum):
    DISABLED = "DISABLED"

    DISCONNECTING = "DISCONNECTING"
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"

    VALID = "VALID"

    NOT_AVAILABLE = "NOT_AVAILABLE"

    READ_ERROR = "READ_ERROR"
    WRITE_ERROR = "WRITE_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    ARGUMENT_SYNTAX_ERROR = "ARGUMENT_SYNTAX_ERROR"

    def __str__(self):
        return str(self.value)
