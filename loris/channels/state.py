# -*- coding: utf-8 -*-
"""
    loris.core.channels.state
    ~~~~~~~~~~~~~~~~~~~~~~~~~


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

    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    ARGUMENT_SYNTAX_ERROR = "ARGUMENT_SYNTAX_ERROR"
