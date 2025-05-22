# -*- coding: utf-8 -*-
"""
lori.data.typing
~~~~~~~~~~~~~~~~


"""

from typing import Iterable, TypeVar

from lori.data import Channel, Channels

ChannelsType = TypeVar("ChannelsType", Channel, Channels, Iterable[Channel], Iterable[str], str)
