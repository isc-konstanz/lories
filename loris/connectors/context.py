# -*- coding: utf-8 -*-
"""
loris.connectors.context
~~~~~~~~~~~~~~~~~~~~~~~~


"""

from __future__ import annotations

import os
from collections import OrderedDict
from collections.abc import Mapping
from typing import Collection, Dict, Iterator

from loris import ConfigurationException, Configurations, Configurator
from loris.connectors import Connector, ConnectorException, registry


class ConnectorContext(Configurator, Mapping[str, Connector]):
    __connectors: Dict[str, Connector]

    def __init__(self, configs: Configurations, *args, **kwargs) -> None:
        super().__init__(configs, *args, **kwargs)
        self.__connectors = OrderedDict()

    def __iter__(self) -> Iterator[str]:
        return iter(self.__connectors)

    def __len__(self) -> int:
        return len(self.__connectors)

    def __getitem__(self, uuid: str) -> Connector:
        return self.__get(uuid)

    def __contains__(self, uuid) -> bool:
        return uuid in self.__connectors.keys()

    def configure(self, configs: Configurations) -> None:
        self._do_load_from_file(configs.dirs.conf)

    def _do_configure_members(self, configurators: Collection[Configurator]) -> None:
        configurators = list(configurators)
        configurators.extend([c for c in self.__connectors.values() if c not in configurators])
        super()._do_configure_members(configurators)

    # noinspection PyTypeChecker, PyProtectedMember, PyUnresolvedReferences
    def _do_load_from_file(
        self,
        configs_dir: str,
        configs_file: str = "connectors.conf",
        prefix_uuid: str = None
    ) -> None:
        configs_path = os.path.join(configs_dir, configs_file)
        if os.path.isfile(configs_path):
            configs_dirs = self.configs.dirs.encode()
            configs_dirs["conf_dir"] = configs_dir
            configs = Configurations.load(configs_file, **configs_dirs)
            self._do_load_sections(configs, prefix_uuid)

    def _do_load_sections(
        self,
        configs: Configurations,
        prefix_uuid: str = None
    ) -> None:
        connector_ids = [i for i in configs.keys() if isinstance(configs[i], Mapping)]
        connectors = {i: configs.pop(i) for i in connector_ids}
        for connector_id, connector_section in connectors.items():
            connector_uuid = connector_id if prefix_uuid is None else f"{prefix_uuid}.{connector_id}"
            connector_configs = configs.copy()
            connector_configs.update(connector_section)
            connector_configs.set("id", connector_id)
            connector_configs.set("uuid", connector_uuid)
            connector = self._new(connector_configs)
            if connector.uuid in self:
                self.__get(connector.uuid).configs.update(connector_configs)
            else:
                self._add(connector)

    def __get(self, uuid: str) -> Connector:
        return self.__connectors.get(uuid)

    # noinspection SpellCheckingInspection
    def _new(self, configs: Configurations) -> Connector:
        if "id" not in configs:
            configs.set("id", os.path.splitext(configs.name)[0])
            configs.move_to_top("id")

        registration_type = configs.get("type").lower()
        if registration_type not in registry.types.keys():
            raise ConnectorException(f"Invalid connector type: {registration_type}")

        return registry.types[registration_type].initialize(self, configs)

    def _add(self, connector: Connector) -> None:
        if not isinstance(connector, Connector):
            raise ConnectorException(f"Invalid connector type: {type(connector)}")

        if connector.uuid in self.__connectors.keys():
            raise ConfigurationException(f'Connector with UUID "{connector.uuid}" already exists')

        self.__connectors[connector.uuid] = connector

    def _remove(self, uuid: str) -> None:
        del self.__connectors[uuid]
