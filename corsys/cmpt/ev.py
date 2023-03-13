# -*- coding: utf-8 -*-
"""
    corsys.cmpt.ev
    ~~~~~~~~~~~~~~


"""
from ..configs import Configurations
from . import Component


class ElectricVehicle(Component):

    # noinspection PyProtectedMember
    def __configure__(self, configs: Configurations) -> None:
        super().__configure__(configs)
        self._capacity = configs.getfloat('EV', 'capacity', fallback=30)

    @property
    def type(self) -> str:
        return 'ev'

    @property
    def capacity(self) -> float:
        return self._capacity
