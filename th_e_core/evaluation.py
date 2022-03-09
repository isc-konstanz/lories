# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import MutableSequence, MutableMapping
from typing import Dict, List, Tuple, Iterator

import os
import re
import logging
import pandas as pd

from configparser import ConfigParser as Configurations
from configparser import SectionProxy
from th_e_core import Configurable, Database

logger = logging.getLogger(__name__)

INVALID_CHARS = "'!@#$%^&?*;:,./\|`´+~=- "


class Evaluations(MutableSequence):

    def __init__(self, *evaluations: Evaluation) -> None:

        self._evaluations = list()
        self._evaluations.extend(evaluations)

    def __iter__(self) -> Iterator[Evaluation]:
        return iter(self._evaluations)

    def __len__(self) -> int:
        return len(self._evaluations)

    def __getitem__(self, index: int) -> Evaluation:
        return self._evaluations[index]

    def __delitem__(self, index: int) -> None:
        del self._evaluations[index]

    def __setitem__(self, index: int, system: Evaluation) -> None:
        self._evaluations[index] = system

    def insert(self, index: int, system) -> None:
        self._evaluations.insert(index, system)

    def run(self, *args, **kwargs) -> None:
        for evaluation in self:
            evaluation.run(*args, **kwargs)


class Evaluation(Configurable):

    def __init__(self, name, configs: Configurations, **kwargs) -> None:

        self.name = name

        if self.name not in configs.sections():
            raise ValueError('The requested evaluation {} is not present in the configs.'.format(self.name))

        super().__init__(configs, **kwargs)

        attr_names = {'targets', 'metrics', 'groups', 'summaries', 'conditions', 'boxplots'}

        if not attr_names.issubset(set(configs[self.name].keys())):
            raise ValueError('Invalid configuration, missing attribute in configs.'
                             ' Check that the following are present {}'.format(attr_names))

        # Properties
        self.targets = configs[self.name]['targets']
        self.metrics = configs[self.name]['metrics']
        self.groups = configs[self.name]['groups']
        self.boxplots = configs[self.name]['boxplots']
        self.conditions = configs[self.name]['conditions']
        self.summaries = configs[self.name]['summaries']

        self._activate(configs, **kwargs)

    @property
    def targets(self):
        return self._targets

    @targets.setter
    def targets(self, value):

        values = value.split(', ')
        new_values = list()

        while values:

            t = values.pop().lower()

            # Ensure proper formatting of target string (no special signs or spaces)
            # ToDo: Ensure that name is still a valid variable in the future
            if re.match('[^a-zA-Z0-9-_]+', t):
                raise ValueError('An improper target name was passed for the evaluation {}'.format(self.name))

            new_values.append(t)

        self._targets = new_values

    @property
    def metrics(self):
        return self._metrics

    @metrics.setter
    def metrics(self, value):

        values = value.split(', ')
        new_values = list()

        while values:

            t = values.pop().lower()

            # ToDo: Ensure that name is still a valid variable in the future
            if re.match('[^a-zA-Z0-9-_]+', t):
                raise ValueError('An improper metric name was passed for the evaluation {}'.format(self.name))

            new_values.append(t)

        self._metrics = new_values

    @property
    def groups(self):
        return self._groups

    @groups.setter
    def groups(self, value):

        values = value.split(', ')
        new_values = list()

        while values:

            t = values.pop().lower()

            # Ensure proper formatting of target string (no special signs or spaces)
            # ToDo: Ensure that name is still a valid variable in the future
            if re.match('[^a-zA-Z0-9-_]+', t):
                raise ValueError('An improper target name was passed for the evaluation {}'.format(self.name))

            new_values.append(t)

        self._groups = new_values

    @property
    def boxplots(self):
        return self._boxplots

    @boxplots.setter
    def boxplots(self, value):

        values = value.split(', ')
        new_values = list()

        while values:

            t = values.pop().lower()
            truth = ['1', 'true', 'yes']

            if t in truth:
                new_values.append(True)
            else:
                new_values.append(False)

        self._boxplots = new_values

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):

        values = value.split(', ')
        new_values = list()

        while values:

            t = values.pop().lower()

            # Ensure proper formatting of target string (no special signs or spaces)
            # ToDo: Ensure that name is still a valid variable in the future
            if re.match('[^a-zA-Z0-9-_><=.,]+', t):
                raise ValueError('The condition {} in the evaluation {} contains unsupported characters'.format(t, self.name))

            t = t.split(' ')
            t[2] = int(t[2])
            new_values.append(t)

        self._conditions = new_values

    @property
    def summaries(self):
        return self._summaries

    @summaries.setter
    def summaries(self, value):

        valid_sum = {'horizon_weighted', 'mean', 'high_load_bias',
                     'err_per_load', 'optimist', 'fullest_bin'}

        values = value.split('; ')
        new_values = list()

        while values:

            t = values.pop().lower()

            t = t.split(', ')

            if not set(t).issubset(valid_sum):
                raise ValueError('An improper summary was passed for the evaluation {}. Please ensure'
                                 ' that all chosen summaries are present in the following list:'
                                 ' {}'.format(self.name, list(valid_sum)))

            new_values.append(t)

        self._summaries = new_values

    def _configure(self, configs: Configurations, **kwargs) -> None:
        # super()._configure(configs, **kwargs)
        pass

    def _activate(self, configs: Configurations, **kwargs) -> None:
        if configs.has_section('Database') and \
                configs.get('Database', 'enabled', fallback='True').lower() == 'true' and \
                configs.get('Database', 'enable', fallback='True').lower() == 'true':

            if 'dir' in configs['Database']:
                database_dir = configs['Database']['dir']
                if not os.path.isabs(database_dir):
                    configs['Database']['dir'] = os.path.join(configs['General']['data_dir'], database_dir)
            else:
                configs['Database']['dir'] = configs['General']['data_dir']

            self._database = Database.open(configs, **kwargs)
        else:
            self._database = None

    @classmethod
    def read(cls, data_dir: str = 'data', config_scan: bool = False, **kwargs) -> Evaluations:

        if not isinstance(config_scan, bool):
            config_scan = str(config_scan).lower() == 'true'
        kwargs['config_scan'] = config_scan

        if config_scan:
            for system_dir in os.scandir(data_dir):
                if os.path.isdir(system_dir.path):
                    configs = cls._read(data_dir=system_dir.path, **kwargs)
                    evaluations = Evaluations(*cls._from_configs(configs, **kwargs))
        else:
            configs = cls._read(data_dir=data_dir, **kwargs)
            evaluations = Evaluations(*cls._from_configs(configs))

        return evaluations

    @classmethod
    def _read(cls,
              root_dir:    str = '.',
              lib_dir:     str = 'lib',
              tmp_dir:     str = 'tmp',
              data_dir:    str = 'data',
              config_dir:  str = 'conf',
              config_name: str = None,
              **kwargs) -> Configurations:

        if config_name is None:
            config_name = cls.__name__.lower() + '.cfg'

        configs = cls._read_configs(root_dir, lib_dir, tmp_dir, data_dir, config_dir, config_name, **kwargs)

        package = kwargs.get('package') if 'package' in kwargs else '.'.join(cls.__module__.split('.')[:-1])
        module = kwargs.get('module') if 'module' in kwargs else cls.__module__.split('.')[-1]

        if 'Import' not in configs.sections():
            configs.add_section('Import')

        if configs.has_option('General', 'type') and not configs.get('General', 'type').lower() == 'default':
            configs.set('Import', 'class', configs.get('General', 'type'))
        elif not configs.has_option('Import', 'class'):
            configs.set('Import', 'class', cls.__name__)
        if not configs.has_option('Import', 'module'):
            configs.set('Import', 'module', module)
        if not configs.has_option('Import', 'package'):
            configs.set('Import', 'package', package)

        return configs

    @staticmethod
    def _from_configs(configs: Configurations, *args, **kwargs) -> List[Evaluation]:

        evaluations = list()

        try:
            obj = __import__(configs['Import']['package']+'.'+configs['Import']['module'],
                             fromlist=[configs['Import']['class']])

        except ModuleNotFoundError as error:
            logger.debug(error)

            configs.set('Import', 'package', 'th_e_core')
            obj = __import__('th_e_core.'+configs['Import']['module'], fromlist=[configs['Import']['class']])

        if configs.has_option('General', 'names'):

            names = configs['General']['names'].split(', ')
            for name in names:

                evaluations.append(getattr(obj, configs['Import']['class'])(name, configs, *args, **kwargs))
        else:
            raise ValueError("Invalid configuration, missing subsection names in section 'General'.")

        return evaluations

    def run(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

