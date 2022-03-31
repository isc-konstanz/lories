# -*- coding: utf-8 -*-
"""
    th-e-core.system
    ~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import MutableMapping
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


class Evaluations(MutableMapping, Configurable):

    def __init__(self, configs: Configurations, *evaluations, **kwargs) -> None:

        Configurable.__init__(self, configs, **kwargs)

        # ToDo: Datapaths and items and their corresponding system contained
        # in eval_map are currently connected solely by index position. This
        # is not very reliable and should be changed.
        self.data_paths = self._configs['Database']['dirs']
        self.eval_map = dict(self._configs['Evaluation Map'].items())

        # set in load_results
        self._database = None
        #self.load_results()

        # ToDo: check each name is present
        # if self.name not in configs.sections():
        #     raise ValueError('The requested evaluation {} is not present in the configs.'.format(self.name))

        self._evaluations = dict()
        for eval in evaluations:
            self._evaluations[eval.name] = eval

    def __iter__(self) -> Iterator[Evaluation]:
        return iter(self._evaluations)

    def __len__(self) -> int:
        return len(self._evaluations)

    def __getitem__(self, key: str) -> Evaluation:
        return self._evaluations[key]

    def __delitem__(self, key: str) -> None:
        del self._evaluations[key]

    def __setitem__(self, key: str, evaluation: Evaluation) -> None:
        self._evaluations[key] = evaluation

    def _configure(self, configs: Configurations, **kwargs) -> None:
        # super()._configure(configs, **kwargs)
        pass

    @property
    def data_paths(self):
        return self._data_paths

    @data_paths.setter
    def data_paths(self, value):

        _paths = list()
        paths = value.split(', ')

        # ToDo: Allow for relative paths, as in configs.py
        for path in paths:
            if not os.path.isdir(path) or not os.path.isabs(path):
                raise OSError('Path {} not found. Relative paths are currently not supported.'.format(path))

        _paths.extend(paths)
        self._data_paths = _paths

    @property
    def eval_map(self):
        return self._eval_map

    @eval_map.setter
    def eval_map(self, value: dict):
        #ToDo: Consider restricting inputs further
        map = dict()
        for sys_id, evals in value.items():
            evals = evals.split(', ')
            map[sys_id] = evals

        self._eval_map = map

    @classmethod
    def read(cls, conf_dir: str, **kwargs) -> Evaluations:

        configs = cls._read(config_dir=conf_dir, data_dir='data', **kwargs)
        evaluations = Evaluations(configs, *cls._from_configs(configs))

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

        if configs.has_section('Evaluation Map'):

            names = list()
            for dir, eval_name in configs['Evaluation Map'].items():

                names.extend(eval_name.split(', '))

            names = list(set(names))
            for name in names:

                eval_configs = dict(configs[name].items())
                evaluations.append(getattr(obj, configs['Import']['class'])(name,  **eval_configs))

        else:
            raise ValueError("Invalid configuration, missing subsection names in section 'General'.")

        return evaluations

    def load_results(self, path):

        data_path = os.path.join(path, 'results.h5')

        if not os.path.isfile(data_path):
            raise FileExistsError("The requisite file {} does not exist.".format(data_path))

        datastore = pd.HDFStore(data_path)
        results = pd.DataFrame()

        for date_path in datastore:

            if date_path.endswith('outputs'):

                result = datastore.get(date_path)
                results = pd.concat([results, result], axis=0)

        results = results.sort_index()
        results['day_hour'] = [t.hour for t in results.index]
        results['weekday'] = [t.weekday for t in results.index]
        results['month'] = [t.month for t in results.index]

        # ToDo: Move check to appropriate location in Evaluation class
        #if not self._cols.issubset(set(results.columns)):
        #    _na = self._cols.difference(set(results.columns))
        #    raise ValueError("Unable to load data corresponding to the path {}, as the"
        #                     " columns {} required for the evaluation {} were not present"
        #                     ".".format(data_path, _na, self.name))

        results.index = [i for i in range(len(results))]

        self._database = results
        datastore.close()

    def run(self) -> None:
        from copy import deepcopy

        i = 0
        for sys_id, evaluations in self.eval_map.items():

            self.load_results(self.data_paths[i])
            for e_id in evaluations:

                data = deepcopy(self._database)
                self[e_id].run(sys_id, data)

            i += 1


class Evaluation:

    def __init__(self, eval_name, targets, metrics, groups, group_bins=None, conditions=None, summaries=None, boxplots=None, **kwargs) -> None:

        # Run invariant, necessary
        self.name = eval_name
        self.targets = targets
        self.metrics = metrics
        self.groups = groups

        # Run invariant, optional
        self.group_bins = group_bins
        self.boxplots = boxplots
        self.conditions = conditions
        self.summaries = summaries

        # Outputs, cumulative: Multiple runs will concatenate
        # their outputs to these attributes
        self.systems = list()
        self.evaluation = pd.DataFrame()
        self.kpi = pd.DataFrame()
        self.n = pd.DataFrame()  # Count the points in each group, as defined in groups

        # Run specific variables
        self._data = None
        self.data = None
        self.system = None  # To assess current state of run specific variable

        _cols = list()
        if conditions:
            for condition in self.conditions:
                _cols.append(condition[0])

        _err_cols = [t + '_err' for t in self.targets]
        _cols = _cols + _err_cols + self.groups
        self._cols = set(_cols)

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
    def group_bins(self):
        return self._group_bins

    @group_bins.setter
    def group_bins(self, value):

        if not value:
            self._group_bins = None

        else:
            values = value.split(', ')
            new_values = list()

            while values:

                t = values.pop().lower()
                t = int(t)
                new_values.append(t)

            self._group_bins = new_values

    @property
    def boxplots(self):
        return self._boxplots

    @boxplots.setter
    def boxplots(self, value):

        if not value:
            self._boxplots = None

        else:

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

        if not value:
            self.conditions = None

        else:
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

        if not value:
            self._summaries = None

        else:
            valid_sum = ['horizon_weighted', 'mean', 'high_load_bias',
                         'err_per_load', 'optimist', 'fullest_bin']

            values = value.split(', ')
            new_values = list()

            while values:

                t = values.pop().lower()

                if t not in valid_sum:
                    raise ValueError('The summary {} passed for the evaluation {} is not valid. Please ensure'
                                     ' that all chosen summaries are present in the following list:'
                                     ' {}'.format(t, self.name, valid_sum))

                new_values.append(t)

            self._summaries = new_values

    def _extract_labels(self):

        def _gitterize(data: pd.Series, steps):
            from math import floor, ceil

            total_steps = steps
            f_max = ceil(data.max())
            f_min = floor(data.min())
            big_delta = f_max - f_min

            # Round step_size down
            small_delta = floor(big_delta / total_steps * 10) / 10

            if small_delta == 0:
                raise ValueError("The axis {} cannot be analyzed with the regular grid spacing of {} between"
                                 "grid points. Please choose a smaller number of steps".format(feature, small_delta))

            to_edge = big_delta - small_delta * total_steps
            extra_steps = ceil(to_edge / small_delta)
            total_steps = total_steps + extra_steps

            discrete_axis = [round(f_min + small_delta * x, 2) for x in range(total_steps + 1)]

            return discrete_axis, small_delta

        if len(self.groups) != len(self.group_bins):
            _groups = self.groups[:len(self.group_bins)].copy()
        else:
            _groups = self.groups.copy()

        # Update self.groups to appropriate bin identifier
        self.groups[:len(_groups)] = [group + '_bins' for group in _groups]

        d_axis = zip(_groups, self.group_bins)
        gitterized = list()

        for feature, steps in d_axis:

            self.data[feature + '_bins'] = self.data[feature]
            discrete_feature, step_size = _gitterize(self.data[feature], int(steps))
            gitterized.append(feature)

            for i in discrete_feature:
                i_loc = self.data[feature + '_bins'] - i
                i_loc = (i_loc >= 0) & (i_loc < step_size)
                self.data.loc[i_loc, feature + '_bins'] = i

            # drop unbinned data
            self.data = self.data.drop(feature, 1)

    def _select_data(self):

        def select_rows(data, feature, operator, value, *args):

            series = data[feature]

            if operator.lower() in ['lt', '<']:
                rows = (series < value)

            elif operator.lower() in ['gt', '>']:
                rows = (series > value)

            elif operator.lower() in ['leq', '<=']:
                rows = (series <= value)

            elif operator.lower() in ['geq', '>=']:
                rows = (series >= value)

            elif operator.lower() in ['eq', '=', '==']:
                rows = (series == value)

            else:
                raise ValueError('An improper condition is present in the dict defining the kpi.')

            return rows

        _ps = pd.Series([True] * len(self.data), index=self.data.index)

        for c in self.conditions:

            if not c:
                continue

            rows = select_rows(self.data, *c)
            _ps = _ps & rows

            if not c[0] in self.groups:
                self.data = self.data.drop(c[0], 1)

        self.data = self.data.iloc[_ps.values]

    def prepare_data(self, data):

        self._data = data
        self.data = self._data[self._cols]

        if self.conditions:
            self._select_data()

        if self.group_bins:
            self._extract_labels()

    def perform_metric(self, data, target, metric):

        err_col = target + '_err'
        _metrics = []

        if 'mae' == metric:

            data[err_col] = data[err_col].abs()
            mae = data.groupby(self.groups).mean()
            ae_std = data.groupby(self.groups).std()
            _metrics.append(mae)
            _metrics.append(ae_std)

        elif 'mse' == metric:

            data[err_col] = (data[err_col] ** 2)
            mse = data.groupby(self.groups).mean()
            se_std = data.groupby(self.groups).std()
            _metrics.append(mse)
            _metrics.append(se_std)

        elif 'rmse' == metric:

            data[err_col] = (data[err_col] ** 2)
            rmse = data.groupby(self.groups).mean() ** 0.5
            rse_std = data.groupby(self.groups).std() ** 0.5
            _metrics.append(rmse)
            _metrics.append(rse_std)

        elif 'mbe' == metric:

            mbe = data.groupby(self.groups).mean()
            be_std = data.groupby(self.groups).std()
            _metrics.append(mbe)
            _metrics.append(be_std)

        else:
            raise ValueError("The chosen metric {} has not yet been implemented".format(metric))

        # concatenate results
        metric_data = pd.concat(_metrics, axis=1)
        metric_data.columns = [metric, metric + '_std']

        return metric_data

    def summarize(self, data: pd.Series, summary):

        options = ['horizon_weighted', 'mean', 'high_load_bias',
                   'err_per_load', 'optimist', 'fullest_bin']

        w = pd.Series([4 / 7, 2 / 7, 1 / 7], name='weights')

        if summary == 'mean':

            kpi = data.mean()
            name = data.name + '_' + summary
            kpi = pd.Series([kpi], index=[0], name=name)

        elif summary == 'horizon_weighted':

            # ToDo: This can be implemented for MultiIndexed series by 'integrating' over other levels
            if 'horizon' not in data.index.names or isinstance(data.index, pd.MultiIndex):
                raise ValueError("This summary is not compatible with your "
                                 "chosen group index {}".format(self.groups))
            ri = [9, 12, 16]

            w.index = ri
            kpi = (data.loc[ri]).dot(w)

            name = data.name + '_' + summary
            kpi = pd.Series([kpi], index=[0], name=name)

        elif summary == 'high_load_bias':

            # This calculation only works as long as the following assumption
            # is true: The error scales with target load
            qs = data.quantile([0.75, 0.5, 0.25])
            qs.index = w.index
            kpi = qs.dot(w)

            name = data.name + '_' + summary
            kpi = pd.Series([kpi], index=[0], name=name)

        elif summary == 'err_per_load':

            watt_series = pd.Series(data.index, index=data.index)
            watt_series = watt_series.iloc[(watt_series != 0).values]
            err_watt = data.loc[watt_series.index, :].div(watt_series)
            kpi = err_watt.mean()

            name = data.name + '_' + summary
            kpi = pd.Series([kpi], index=[0], name=name)

        elif summary == 'optimist':

            kpi = data.min()

            name = data.name + '_' + summary
            kpi = pd.Series([kpi], index=[0], name=name)

        elif summary == 'fullest_bin':

            if isinstance(data.index, pd.MultiIndex) or data.name != 'count':
                raise AttributeError("This summary has not yet been implemented for multiindexed bins.")

            if data.name != 'count':
                raise ValueError("This summary is only appropriate for count data.")

            kpi = data.idxmax()

            name = summary
            kpi = pd.Series([kpi], index=[0], name=name)

        else:
            raise ValueError('The current option is not yet available for metric summarization '
                             'please choose one of the following options: {}'.format(options))

        return kpi

    def run(self, eval_id: str, data: pd.DataFrame):
        from copy import deepcopy

        self.system = eval_id
        self.systems.append(eval_id)
        self.prepare_data(data)

        cols = [col for col in self.data.columns if not col.endswith('_err')]

        # calculate count
        n = [1 for x in range(len(self.data))]
        n = pd.Series(n, index=self.data.index, name=self.system)
        n = pd.concat([self.data[self.groups], n], axis=1)
        n = n.groupby(self.groups).sum()
        self.n = pd.concat([self.n, n], axis=1)

        for target in self.targets:

            cols.append(target + '_err')
            data = deepcopy(self.data[cols])

            i = 0
            for metric in self.metrics:

                # calculate output
                summary = self.summaries[i]
                metric_data = self.perform_metric(data, target, metric)
                kpi = self.summarize(metric_data[metric], summary)

                # Format output
                metric_data.columns = pd.MultiIndex.from_product([[target], metric_data.columns, [self.system]],
                                                                 names=['targets', 'metrics', 'systems'])
                kpi = pd.DataFrame(kpi)
                kpi.columns = pd.MultiIndex.from_tuples([(target, summary, self.system)],
                                                        names=['targets', 'summaries', 'systems'])

                # Save output
                self.evaluation = pd.concat([self.evaluation, metric_data], axis=1)
                self.kpi = pd.concat([self.kpi, kpi], axis=1)
                i += 1

            cols.pop()
