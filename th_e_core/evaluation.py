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

        req = {'targets', 'metrics', 'groups'}
        #opt = {'conditions', 'summaries', 'boxplots'}

        if not req.issubset(set(configs[self.name].keys())):
            raise ValueError('Invalid configuration, missing attribute in configs.'
                             ' Check that the following are present {}'.format(req))

        # necessary
        self.targets = configs[self.name]['targets']
        self.metrics = configs[self.name]['metrics']
        self.groups = configs[self.name]['groups']

        # optional
        if configs.has_option(self.name, 'group_bins'):
            self.group_bins = configs[self.name]['group_bins']

        if configs.has_option(self.name, 'boxplots'):
            self.boxplots = configs[self.name]['boxplots']

        if configs.has_option(self.name, 'conditions'):
            self.conditions = configs[self.name]['conditions']

        if configs.has_option(self.name, 'summaries'):
            self.summaries = configs[self.name]['summaries']

        self._activate(configs, **kwargs)
        self.results = None

        # Outputs
        self.evaluation = pd.DataFrame()
        self.kpi = pd.DataFrame()

        # private
        _cols = list()
        if self.configs.has_option(self.name, 'conditions'):
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

    def load_results(self):
        from warnings import warn
        data_path = os.path.join(self._database.dir, 'results.h5')

        if not os.path.isfile(data_path):
            raise FileExistsError("The requisite file {} does not exist.".format(data_path))

        datastore = pd.HDFStore(data_path)
        results = pd.DataFrame()

        for date_path in datastore:

            if date_path.endswith('outputs'):

                result = datastore.get(date_path)
                if self._cols.issubset(set(result.columns)):
                    results = pd.concat([results, result], axis=0)
                else:
                    _na = self._cols.difference(set(result.columns))
                    warn("Unable to load data corresponding to the path {}, as the"
                         " columns {} required for the evaluation {} were not present"
                         ".".format(data_path, _na, self.name))
                    continue

        results = results.sort_index()

        results['day_hour'] = [t.hour for t in results.index]
        results['weekday'] = [t.weekday for t in results.index]
        results['month'] = [t.month for t in results.index]

        results.index = [i for i in range(len(results))]
        self.results = results

        self._database.close()
        datastore.close()

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

            self.results[feature + '_bins'] = self.results[feature]
            discrete_feature, step_size = _gitterize(self.results[feature], int(steps))
            gitterized.append(feature)

            for i in discrete_feature:
                i_loc = self.results[feature + '_bins'] - i
                i_loc = (i_loc >= 0) & (i_loc < step_size)
                self.results.loc[i_loc, feature + '_bins'] = i

    def _discrete_metrics(self, target, boxplot=False, **kwargs):

        #ToDo: Move this prepatory code to appropriate location.

        # replace continuous groups with discretized equivalents generated in _extract_labels
        if self._configs.has_option(self.name, 'group_bins'):
            self._extract_labels()

        def perform_metrics(name, data, err_col, groups, metrics, boxplot):

            from copy import deepcopy
            data = deepcopy(data)
            _metrics = []
            for metric in metrics:

                if 'mae' == metric:

                    data[err_col] = data[err_col].abs()
                    mae = data.groupby(groups).mean()
                    ae_std = data.groupby(groups).std()
                    _metrics.append(mae)
                    _metrics.append(ae_std)

                elif 'mse' == metric:

                    data[err_col] = (data[err_col] ** 2)
                    mse = data.groupby(groups).mean()
                    se_std = data.groupby(groups).std()
                    _metrics.append(mse)
                    _metrics.append(se_std)

                elif 'rmse' == metric:

                    data[err_col] = (data[err_col] ** 2)
                    rmse = data.groupby(groups).mean() ** 0.5
                    rse_std = data.groupby(groups).std() ** 0.5
                    _metrics.append(rmse)
                    _metrics.append(rse_std)

                elif 'mbe' == metric:

                    mbe = data.groupby(groups).mean()
                    be_std = data.groupby(groups).std()
                    _metrics.append(mbe)
                    _metrics.append(be_std)

                else:
                    raise ValueError("The chosen metric {} has not yet been implemented".format(metric))

                #if boxplot and len(groups) == 1:
                    #_print_boxplot(system, data[groups[0]], data[err_col].values, os.path.join("evaluation", name, metric))

            # introduce count to data
            n = [1 for x in range(len(data))]
            n = pd.Series(n, index=data.index, name='count')
            data = pd.concat([data, n], axis=1)

            # count points in each group
            n = data[groups + ['count']].groupby(groups).sum()

            _metrics.append(n)

            # concatenate results
            metric_data = pd.concat(_metrics, axis=1)

            # Generate appropriate column names
            metrics_c1 = [metric for metric in metrics]
            metrics_c2 = [metric + '_std' for metric in metrics]
            metric_cols = list()

            for metric, std in zip(metrics_c1, metrics_c2):

                metric_cols.append(metric)
                metric_cols.append(std)

            metric_cols.append('count')
            metric_data.columns = metric_cols

            return metric_data

        def select_data(data, conditions):

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

            _ps = pd.Series([True] * len(data), index=data.index)


            for c in conditions:

                if not c:
                    continue

                rows = select_rows(data, *c)
                _ps = _ps & rows

            selected = data.iloc[_ps.values]

            return selected

        def summarize(evaluation, metric, groups, option=None):

            options = ['horizon_weighted', 'mean', 'high_load_bias',
                       'err_per_load', 'optimist', 'fullest_bin']

            w = pd.Series([4 / 7, 2 / 7, 1 / 7], name='weights')

            if option == 'mean':

                return evaluation[metric].mean()

            elif option == 'horizon_weighted':

                if not 'horizon' in groups:
                    raise ValueError("This summary is not compatible with your "
                                     "chosen group index {}".format(groups))
                ri = [1, 3, 6]
                w.index = ri
                weighted_sum = evaluation.loc[ri, metric].dot(w)
                return weighted_sum

            elif option == 'high_load_bias':

                # This calculation only works as long as the following assumption
                # is true: The error scales with target load
                qs = evaluation[metric].quantile([0.75, 0.5, 0.25])
                qs.index = w.index
                weighted_sum = qs.dot(w)

                return weighted_sum

            elif option == 'err_per_load':

                watt_series = pd.Series(evaluation.index, index=evaluation.index)
                watt_series = watt_series.iloc[(watt_series != 0).values]
                err_watt = evaluation.loc[watt_series.index, metric].div(watt_series)
                err_watt = err_watt.mean()
                return err_watt

            elif option == 'optimist':

                return evaluation[metric].min()

            elif option == 'fullest_bin':

                if isinstance(evaluation.index, pd.MultiIndex):
                    raise AttributeError("This summary has not yet been implemented for multiindexed bins.")

                candidate = evaluation['count'].idxmax()
                return candidate

            else:

                raise ValueError('The current option is not yet available for metric summarization '
                                  'please choose one of the following options: {}'.format(options))

        # select err data pertaining to desired target
        err_col = target + '_err'
        eval_cols = [err_col] + self.groups

        #select data pertaining to the desired feature space to be examined
        if self._configs.has_option(self.name, 'conditions'):
            data = select_data(self.results, self.conditions)
        else:
            data = self.results

        data = data[eval_cols]

        # calculate metrics
        evaluation = perform_metrics(self.name, data, err_col, self.groups, self.metrics, boxplot)
        kpi = summarize(evaluation, self.metrics[0], self.groups, option=self.summaries[0][0])
        kpi = pd.DataFrame([kpi], index=[0], columns=[self.summaries[0][0]])

        self.evaluation = pd.concat([self.evaluation, evaluation], axis=0)
        self.kpi = pd.concat([self.kpi, kpi], axis=0)

    def run(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

