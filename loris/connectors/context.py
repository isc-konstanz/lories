# -*- coding: utf-8 -*-
"""
    loris.connectors.context
    ~~~~~~~~~~~~~~~~~~~~~~~~
    
    
"""
from __future__ import annotations
from collections.abc import Mapping
from typing import Iterator

import os
import logging

from collections import OrderedDict
from loris import Configurable, Configurations, ConfigurationException
from loris.connectors import Connector, ConnectorException, ConnectorRegistration, registry
from loris.connectors.csv import CsvConnector

logger = logging.getLogger(__name__)


logger.debug("Registering CSV connector")
registry.types[CsvConnector.TYPE] = ConnectorRegistration(CsvConnector, CsvConnector.TYPE)

try:
    logger.debug("Registering MySQL connector")
    from loris.connectors.mysql import MySqlConnector
    registry.types[MySqlConnector.TYPE] = ConnectorRegistration(MySqlConnector, MySqlConnector.TYPE)

except ImportError as e:
    logger.debug(f"Failed registering MySQL connector: {e}")


class ConnectorContext(Configurable, Mapping[str, Connector]):

    _connectors: OrderedDict[str, Connector] = OrderedDict()

    def __init__(self, context, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self._context = context

    def __configure__(self, configs) -> None:
        super().__configure__(configs)
        self._load_file(configs.dirs.conf)
        for connector in self._connectors.values():
            connector.configure()

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _load_file(self,
                   configs_dir: str,
                   configs_file: str = 'connectors.conf',
                   connector_prefix: str = None) -> None:
        configs_path = os.path.join(configs_dir, configs_file)
        if os.path.isfile(configs_path):
            configs_dirs = self.configs.dirs.encode()
            configs_dirs['conf_dir'] = configs_dir
            configs = Configurations.load(configs_file, **configs_dirs)
            self._load(configs, connector_prefix)

    def _load(self, configs: Configurations, prefix_uuid: str = None) -> None:
        connector_ids = [i for i in configs.keys() if isinstance(configs[i], Mapping)]
        connectors = {
            i: configs.pop(i) for i in connector_ids
        }
        for connector_id, connector_section in connectors.items():
            connector_uuid = connector_id if prefix_uuid is None else f'{prefix_uuid}.{connector_id}'
            connector_configs = configs.copy()
            connector_configs.update(connector_section)
            connector_configs.set('id', connector_id)
            connector_configs.set('uuid', connector_uuid)

            self._add(self._new(connector_configs))

    # noinspection SpellCheckingInspection
    def _new(self, configs: Configurations) -> Connector:
        if 'id' not in configs:
            configs.set('id', os.path.splitext(configs.name)[0])
            configs.move_to_top('id')

        registration_type = configs.get('type').lower()
        if registration_type not in registry.types.keys():
            raise ConnectorException(f"Invalid connector type: {registration_type}")

        return registry.types[registration_type].initialize(self, configs)

    def _add(self, connector: Connector) -> None:
        if not isinstance(connector, Connector):
            raise ConnectorException(f'Invalid connector type: {type(connector)}')

        if connector.uuid in self._connectors.keys():
            raise ConfigurationException(f'Connector with UUID "{connector.uuid}" already exists')

        self._connectors[connector.uuid] = connector

    def _remove(self, uuid: str) -> None:
        del self._connectors[uuid]

    # noinspection PyShadowingBuiltins
    def __getitem__(self, id: str) -> Connector:
        return self._connectors[id]

    def __iter__(self) -> Iterator[str]:
        return iter(self._connectors)

    def __len__(self) -> int:
        return len(self._connectors)
