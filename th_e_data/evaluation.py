# -*- coding: utf-8 -*-
"""
    th-e-data.evaluation
    ~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import Mapping, MutableMapping
from typing import List, Iterator

import os
import re
import json
import shutil
import logging
import pandas as pd
import datetime as dt

from django.utils.datetime_safe import date

import th_e_data.io as io
from th_e_core import configs, System, ConfigurationUnavailableException
from copy import deepcopy

logger = logging.getLogger(__name__)


class Evaluations(Mapping):

    def __init__(self, *evaluations) -> None:
        self._evaluations = dict()
        for evaluation in evaluations:
            self._evaluations[evaluation.id] = evaluation

    def __iter__(self) -> Iterator[Evaluation]:
        return iter(self._evaluations)

    def __len__(self) -> int:
        return len(self._evaluations)

    def __getitem__(self, key: str) -> Evaluation:
        return self._evaluations[key]

    def run(self, results: List[Results]) -> None:
        for result in results:
            for evaluation in self._evaluations.values():
                evaluation.run(result)


class Evaluation:

    @classmethod
    def read(cls, **kwargs) -> Evaluations:
        evaluations = []
        evaluation_configs = configs.read('evaluation.cfg', **kwargs)
        if not os.path.isfile(evaluation_configs):
            config_default = evaluation_configs.replace('.cfg', '.default.cfg')
            if os.path.isfile(config_default):
                shutil.copy(config_default, evaluation_configs)
            else:
                raise ConfigurationUnavailableException('Unable to find configuration file "{}"'
                                                        .format(evaluation_configs))

        for evaluation in evaluation_configs.sections():
            evaluations.append(cls(evaluation, **dict(evaluation_configs[evaluation].items())))

        return Evaluations(evaluations)

    def __init__(self,
                 name,
                 column,
                 metrics,
                 groups,
                 group_bins=None,
                 conditions=None,
                 summaries=None,
                 boxplots=None, **_) -> None:

        # Run invariant, necessary
        self.name = name
        self.columns = column
        self.metrics = metrics
        self.groups = groups

        # Run invariant, optional
        self.group_bins = group_bins
        self.conditions = conditions
        self.summaries = summaries

        self.boxplots = boxplots

        # Outputs, cumulative: Multiple runs will concatenate
        # their outputs to these attributes
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

        _err_cols = [t + '_err' for t in self.columns]
        _cols = _cols + _err_cols + self.groups
        self._cols = set(_cols)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name
        self._id = name.lower()

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = list()

        for column in columns.split(', '):
            # Ensure proper formatting of column string (no special signs)
            if re.match('[^a-zA-Z0-9-_ ]+', column):
                raise ValueError('An improper column name was passed for the evaluation {}'.format(column))

            self._columns.append(column.trim())

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
                                 "grid points. Please choose a smaller number of steps".format(data.name, small_delta))

            to_edge = big_delta - small_delta * total_steps
            extra_steps = ceil(to_edge / small_delta)
            total_steps = total_steps + extra_steps

            discrete_axis = [round(f_min + small_delta * x, 2) for x in range(total_steps + 1)]

            return discrete_axis, small_delta

        # Update self.groups to appropriate bin identifier
        if len(self.systems) == 0:
            bin_groups = self.groups[:len(self.group_bins)]
            self.groups[:len(self.group_bins)] = [group + '_bins' for group in bin_groups]

        d_axis = zip(self.groups[:len(self.group_bins)], self.group_bins)
        gitterized = list()

        for bin_feature, steps in d_axis:

            feature = '_'.join(bin_feature.split('_')[:-1])
            self.data[bin_feature] = self.data[feature]
            discrete_feature, step_size = _gitterize(self.data[feature], int(steps))
            gitterized.append(bin_feature)

            for i in discrete_feature:
                i_loc = self.data[bin_feature] - i
                i_loc = (i_loc >= 0) & (i_loc < step_size)
                self.data.loc[i_loc, bin_feature] = i

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
            err_watt = data.loc[watt_series.index].div(watt_series)
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
        self.prepare_data(data)

        cols = [col for col in self.data.columns if not col.endswith('_err')]

        # calculate count
        n = [1 for x in range(len(self.data))]
        n = pd.Series(n, index=self.data.index, name=self.system)
        n = pd.concat([self.data[self.groups], n], axis=1)
        n = n.groupby(self.groups).sum()
        self.n = pd.concat([self.n, n], axis=1)

        for target in self.columns:

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

        # Record evaluation in history
        self.systems.append(eval_id)


class Durations(Mapping):

    def __init__(self, system: System) -> None:
        self._file = os.path.join(system.configs['General']['data_dir'], 'results', 'results.json')
        if os.path.isfile(self._file):
            with open(self._file, 'r', encoding='utf-8') as f:
                self._durations = json.load(f)
                for duration in self._durations.values():
                    def _datetime(key):
                        return dt.datetime.strptime(duration[key], '%Y-%m-%d %H:%M:%S.%f')

                    if 'start' in duration:
                        duration['start'] = _datetime('start')
                    if 'end' in duration:
                        duration['end'] = _datetime('end')
        else:
            self._durations = {}

    def __repr__(self) -> str:
        return str(self._durations)

    def __iter__(self) -> Iterator[Evaluation]:
        return iter(self._durations)

    def __len__(self) -> int:
        return len(self._durations)

    def __getitem__(self, key: str) -> Evaluation:
        return self._durations[key]['minutes']

    def start(self, key: str) -> None:
        if key not in self._durations:
            self._durations[key] = {}
        if 'minutes' not in self._durations[key]:
            self._durations[key]['minutes'] = 0
        if 'end' in self._durations[key]:
            del self._durations[key]['end']

        self._durations[key]['start'] = dt.datetime.now()

    def stop(self, key: str = None) -> None:
        if key is None:
            for key in self.keys():
                self._stop(key)
        else:
            self._stop(key)

        self._write()

    def _stop(self, key: str = None) -> None:
        if key not in self._durations:
            raise ValueError("No duration found for key: \"{}\"".format(key))
        if 'start' not in self._durations[key]:
            raise ValueError("Timer for key \"{}\" not started yet".format(key))

        self._durations[key]['end'] = dt.datetime.now()

        minutes = self._durations[key]['minutes'] if 'minutes' in self._durations[key] else 0
        minutes += round((self._durations[key]['end'] - self._durations[key]['start']).total_seconds() / 60.0, 6)
        self._durations[key]['minutes'] = minutes

    def _write(self) -> None:
        with open(self._file, 'w', encoding='utf-8') as f:
            json.encoder.FLOAT_REPR = lambda o: format(o, '.3f')
            json.dump(self._durations, f, indent=4, default=str, ensure_ascii=False)


class Results(MutableMapping):

    def __init__(self, system: System, verbose: bool = False) -> None:
        self._system = system
        system_dir = system.configs['General']['data_dir']

        # noinspection PyProtectedMember
        self._database = deepcopy(system._database)
        self._database.dir = os.path.join(system_dir, 'results')
        self._database.enabled = True
        self._datastore = pd.HDFStore(os.path.join(system_dir, 'results', 'results.h5'))

        self.data = pd.DataFrame()
        self.durations = Durations(system)

        self.verbose = verbose

    def __setitem__(self, key: str, data: pd.DataFrame) -> None:
        self.set(key, data)

    def __getitem__(self, key: str) -> pd.DataFrame:
        return self.get(key)

    def __delitem__(self, key: str) -> None:
        del self._datastore[key]

    def __iter__(self):
        return iter(self._datastore)

    def __len__(self) -> int:
        return len(self._datastore)

    def __contains__(self, key: str) -> bool:
        return '/{}'.format(key) in self._datastore

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.durations.stop()
        self._database.close()
        self._datastore.close()
        if self.verbose:
            for results_err in [c for c in self.data.columns if c.endswith('_err')]:
                results_file = os.path.join('results',
                                            'results_' + results_err.replace('_err', '').replace('_power', ''))
                results_data = self.data.reset_index().drop_duplicates(subset='time', keep='last')\
                                        .set_index('time').sort_index()

                io.write_csv(self._system, results_data, results_file)

    def set(self, key: str, data: pd.DataFrame) -> None:
        data.to_hdf(self._datastore, '/{}'.format(key))
        self.data = pd.concat([self.data, data], axis=0)
        if self.verbose:
            self._database.write(data, file='{}.csv'.format(key), rename=False)

    def load(self, key: str) -> pd.DataFrame:
        data = self.get(key)

        self.data = pd.concat([self.data, data], axis=0)
        return data

    # noinspection PyTypeChecker
    def get(self, key: str) -> pd.DataFrame:
        return self._datastore.get('/{}'.format(key))
