# -*- coding: utf-8 -*-
"""
    th-e-data.evaluation
    ~~~~~~~~~~~~~~~~~~~~


"""
from __future__ import annotations
from collections.abc import Sequence, Mapping, MutableMapping
from typing import Tuple, List, Dict, Iterator

import os
import re
import json
import shutil
import logging
import numpy as np
import pandas as pd
import datetime as dt

import th_e_data.io as io
# noinspection PyProtectedMember
from th_e_core.io._var import rename
from th_e_core import configs, System, ConfigurationUnavailableException
from configparser import ConfigParser as Configurations
from math import floor, ceil
from copy import deepcopy

logger = logging.getLogger('th-e-evaluation')

TARGETS = {
    'pv': 'Photovoltaics',
    'el': 'Electrical',
    'th': 'Thermal'
}


class Evaluations(Sequence):

    def __init__(self, evaluations: List[Evaluation], dir: str = 'data') -> None:
        self._evaluations = evaluations
        self._evaluation_dir = dir

    def __iter__(self) -> Iterator[Evaluation]:
        return iter(self._evaluations)

    def __len__(self) -> int:
        return len(self._evaluations)

    def __getitem__(self, index: int) -> Evaluation:
        return self._evaluations[index]

    def _get_valid(self, results: Results) -> List[Evaluation]:
        return [e for e in self._evaluations if e.is_valid(results)]

    def run(self, results: List[Results]) -> None:
        # Prepare the DataFrames, to ensure corrent
        index = []
        duration_columns = []
        evaluation_columns = []
        evaluations = {}
        for result in results:
            if result.name not in index:
                index.append(result.name)
            for duration in result.durations:
                duration_column = ('Durations [min]', str(duration))
                if duration_column not in duration_columns:
                    duration_columns.append(duration_column)
            for evaluation in self._get_valid(result):
                evaluation_columns.append((evaluation.header, evaluation.name))
                evaluations[evaluation.name] = pd.DataFrame()
        columns = duration_columns + [('Total', 'Weighted')]
        for column in list(dict.fromkeys([c for c, _ in evaluation_columns])):
            columns += [c for c in evaluation_columns if c[0] == column]
        summary = pd.DataFrame(index=index, columns=pd.MultiIndex.from_tuples(columns))

        for result in results:
            for duration in result.durations.keys():
                summary.loc[result.name, ('Durations [min]', duration)] = round(result.durations[duration], 2)

            kpi_total_weights = 0
            kpi_total_sum = 0
            for evaluation in self._get_valid(result):
                kpi, kpi_data = evaluation.run(result)
                kpi_data = kpi_data.to_frame()
                kpi_data.columns = [evaluation.header]
                kpi_data.index.name = rename(evaluation.group)
                evaluations[evaluation.name] = pd.concat([evaluations[evaluation.name], kpi_data], axis=1)
                if kpi:
                    summary.loc[result.name, (evaluation.header, evaluation.name)] = kpi

                # TODO: Add meaningful debugging
                kpi_total_weights += evaluation.weight
                kpi_total_sum += kpi*evaluation.weight
            kpi_total = kpi_total_sum/kpi_total_weights

            summary.loc[result.name, ('Total', 'Weighted')] = kpi_total

        io.write_excel(summary, evaluations, self._evaluation_dir)


class Evaluation:

    @classmethod
    def read(cls, config_dir:  str = 'conf', data_dir:  str = 'conf', **kwargs) -> Evaluations:
        evaluations = []
        evaluation_file = os.path.join(config_dir, 'evaluations.cfg')
        if not os.path.isfile(evaluation_file):
            config_default = evaluation_file.replace('.cfg', '.default.cfg')
            if os.path.isfile(config_default):
                shutil.copy(config_default, evaluation_file)
            else:
                raise ConfigurationUnavailableException('Unable to find evaluation file "{}"'.format(evaluation_file))

        evaluation_configs = configs.read(evaluation_file, config_dir, data_dir, **kwargs)
        for evaluation in [s for s in evaluation_configs.sections() if s.lower() != 'general']:
            evaluation_args = dict(evaluation_configs[evaluation].items())
            evaluations += cls._init(evaluation, **evaluation_args)

        return Evaluations(evaluations, data_dir)

    @classmethod
    def _init(cls, name: str, target: str, group: str, **kwargs) -> List[Evaluation]:
        evaluations = []
        for evaluation_target in target.split(','):
            evaluations.append(cls(name, evaluation_target.strip(), group, **kwargs))
        return evaluations

    def __init__(self,
                 name: str,
                 target: str,
                 group: str,
                 group_bins: int = None,
                 condition: str = None,
                 summary: str = 'mbe',
                 metric: str = 'mbe',
                 weight: float = 1,
                 plot: str = None, **_) -> None:

        self.name = name
        self.target = target
        self.group = group
        self.group_bins = group_bins
        self.condition = condition
        self.weight = float(weight)
        self.summary = summary
        self.metric = metric
        self.plot = plot

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name
        self._id = re.sub('[^a-zA-Z0-9-_ ]+', '', name).replace(' ', '_').lower()

    @property
    def columns(self):
        return [self._target,
                self._target+'_est',
                self._target+'_err',
                self.group]

    @property
    def header(self) -> str:
        target = self._target.lower().replace('_power', '')
        if target in TARGETS:
            return TARGETS[target]
        return target.title()

    @property
    def target(self):
        return self._target+'_err'

    @target.setter
    def target(self, target: str) -> None:
        # Ensure proper formatting of string (no special signs)
        if re.match('[^a-zA-Z0-9-_ ]+', target):
            raise ValueError('An improper target name was passed for evaluation {}'.format(t))

        self._target = target.strip()

    @property
    def group(self) -> str:
        group = self._group
        if group == 'histogram':
            group = self.target
        return group

    @group.setter
    def group(self, group: str):
        # Ensure proper formatting of string (no special signs)
        if re.match('[^a-zA-Z0-9-_ ]+', group):
            raise ValueError('An improper group name was passed for evaluation {}'.format(group))

        self._group = group.strip()

    @property
    def group_bins(self) -> int:
        return self._group_bins

    @group_bins.setter
    def group_bins(self, bins: int | str):
        if not bins:
            bins = None
        elif not isinstance(bins, int):
            bins = int(bins)
        self._group_bins = bins

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, condition: str) -> None:
        self._condition = list()
        if not condition:
            return
        for c in condition.split(','):
            # Ensure proper formatting of string (no special signs or spaces)
            # TODO: Ensure that name is still a valid variable in the future
            if re.match('[^a-zA-Z0-9-_><=., ]+', c) or len(re.split('!=|==|>=|<=|<|>', c)) > 2:
                raise ValueError('The condition {} in the evaluation {} contains unsupported characters or operations'
                                 .format(c, self.name))

            self._condition.append(c.strip())

    @property
    def summary(self):
        return self._summary

    @summary.setter
    def summary(self, summary) -> None:
        self._summary = list()
        # Ensure proper formatting of string (no special signs)
        if re.match('[^a-zA-Z0-9-_ ]+', summary):
            raise ValueError('An improper summary name was passed for evaluation {}'.format(summary))

        whitelist = ['mbe',
                     'mae',
                     'rmse',
                     'weight_by_group',
                     'weight_by_bins',
                     'fullest_bin']

        summary = summary.lower().strip()
        if summary not in whitelist:
            raise ValueError('Summary type {} not of available options: {}'.format(summary, whitelist))

        self._summary = summary

    @property
    def metric(self):
        return self._metric

    @metric.setter
    def metric(self, metric: str) -> None:
        # Ensure proper formatting of string (no special signs)
        if re.match('[^a-zA-Z0-9-_ ]+', metric):
            raise ValueError('An improper metric name was passed for evaluation {}'.format(metric))

        whitelist = ['mbe',
                     'mae',
                     'rmse']

        metric = metric.lower().strip()
        if metric not in whitelist:
            raise ValueError('Metric type {} not of available options: {}'.format(metric, whitelist))

        # TODO: verify allowed metrics
        self._metric = metric

    @property
    def plot(self):
        return self._plots

    @plot.setter
    def plot(self, plots: str) -> None:
        self._plots = list()
        if not plots:
            return
        for p in plots.split(','):
            # Ensure proper formatting of string (no special signs)
            if re.match('[^a-zA-Z0-9-_ ]+', p):
                raise ValueError('An improper plot name was passed for evaluation {}'.format(p))

            whitelist = ['line', 'bar']
            p = p.lower().strip()
            if p not in whitelist:
                raise ValueError('Plot type {} not of available options: {}'.format(p, whitelist))

            self._plots.append(p)

    def _plot(self, results: Results, data: pd.DataFrame, evaluation: pd.DataFrame) -> None:
        if len(self.plot) < 1:
            return

        plot_name = '{0}_{1}'.format(self._target.replace('_power', '').lower(), self.id)
        plot_dir = os.path.join(results.system.configs['General']['data_dir'], 'results', 'plots')
        if not os.path.isdir(plot_dir):
            os.makedirs(plot_dir, exist_ok=True)

        for plot in self.plot:
            plot_title = '{0} {1}'.format(self.header, self.name)
            plot_file = os.path.join(plot_dir, '{0}_{1}.png'.format(plot_name, plot))
            plot_args = {'xlabel': rename(self.group), 'title': plot_title}
            if plot == 'line':
                plot_args['hue'] = 'Results'
                plot_args['style'] = 'Results'
                plot_args['ylabel'] = 'Power [W]'
                plot_data = data.rename(columns={
                    self._target+'_est': 'Prediction',
                    self._target:        'Measurement'
                })
                plot_data = plot_data[[self.group, 'Prediction', 'Measurement']]
                plot_melt = pd.melt(plot_data, self.group, value_name='values', var_name='Results')
                io.print_lineplot(plot_melt, self.group, 'values', plot_file, **plot_args)
            elif plot == 'bar':
                if self.group_bins and self.group_bins > 1:
                    if self._group == 'histogram':
                        plot_args['ylabel'] = 'Occurrences'
                        plot_args['xlabel'] = 'Error [W]'
                    plot_data = evaluation.to_frame()
                    plot_index = evaluation.index.values.round(2)
                    io.print_barplot(plot_data, plot_index, self.target, plot_file, **plot_args)
                else:
                    plot_args['showfliers'] = False
                    plot_data = data[[self.group, self.target]]
                    io.print_boxplot(plot_data, self.group, self.target, plot_file, **plot_args)

    def _select(self, results: Results) -> pd.DataFrame:
        data = deepcopy(results.data)
        for condition in self.condition:
            if 'hour' in self.group:
                data['hour'] = data.index.hour
            elif 'day_of_week' in self.group:
                data['day_of_week'] = data.index.day_of_week
            elif 'day_of_year' in self.group:
                data['day_of_year'] = data.index.day_of_year
            elif 'month' in self.group:
                data['month'] = data.index.month

            data.query(condition, inplace=True)
        return data[self.columns]

    def _process(self, data: pd.DataFrame) -> pd.Series:
        data = deepcopy(data)
        if self.group_bins and self.group_bins > 1:
            data = self._process_bins(data)
        else:
            data = getattr(self, '_process_{method}'.format(method=self.metric))(data)

        data.index.name = self.group
        data.name = self.target
        return data

    # noinspection SpellCheckingInspection
    def _process_bins(self, data: pd.DataFrame) -> pd.Series:
        if not self.group_bins or self.group_bins <= 1:
            raise ValueError("Unable to process bins for invalid value: " + str(bins))

        if self.target == self.group:
            bin_data, bin_edges = np.histogram(data[self.target], bins=self.group_bins)
            bin_vals = 0.5*(bin_edges[1:]+bin_edges[:-1])
        else:
            bin_data = []
            bin_vals = []
            _, bin_edges = np.histogram(data[self.group], bins=self.group_bins)
            for bin_left, bin_right in [(bin_edges[i], bin_edges[i+1]) for i in range(self.group_bins)]:
                bin_step = data.loc[(data[self.group] > bin_left) & (data[self.group] <= bin_right), self.target]
                bin_data.append(getattr(self, '_summarize_{method}'.format(method=self.metric))(bin_step))
                bin_vals.append(0.5*(bin_left + bin_right))

        return pd.Series(index=bin_vals, data=bin_data, name=self.target)

    def _process_mbe(self, data: pd.DataFrame) -> pd.Series:
        data = data.groupby(self.group)
        data = data.mean()
        return data[self.target]

    def _process_mae(self, data: pd.DataFrame) -> pd.Series:
        data[self.target] = data[self.target].abs()
        data = data.groupby(self.group)
        data = data.mean()
        return data[self.target]

    def _process_rmse(self, data: pd.DataFrame) -> pd.Series:
        data[self.target] = (data[self.target] ** 2)
        data = data.groupby(self.group)
        data = data.mean() ** .5
        return data[self.target]

    def _summarize(self, data: pd.Series) -> float:
        return getattr(self, '_summarize_{method}'.format(method=self.summary))(data)

    @staticmethod
    def _summarize_mbe(data: pd.Series) -> float:
        return data.mean()

    @staticmethod
    def _summarize_mae(data: pd.Series) -> float:
        return data.abs().mean()

    @staticmethod
    def _summarize_rmse(data: pd.Series) -> float:
        return (data ** 2).mean() ** .5

    @staticmethod
    def _summarize_weight_by_group(data: pd.Series) -> float:
        group_max = data.idxmax()
        group_scaling = np.array([(group_max - i)/group_max for i in data.index])
        group_weight = group_scaling/group_scaling.sum()
        return ((data ** 2) * group_weight).sum() ** .5

    @staticmethod
    def _summarize_weight_by_bins(data: pd.Series) -> float:
        bins_weight = data/data.sum()
        return ((data.index ** 2) * bins_weight).sum() ** .5

    @staticmethod
    def _summarize_fullest_bin(data: pd.Series) -> float | pd.Index:
        return data.idxmax()

    def is_valid(self, results: Results) -> boolean:
        # TODO: Implement calculating error from target and reference instead of expecting column to exist
        if self.target not in results.data.columns:
            return False
        if self.group not in results.data.columns and self.group not in \
                ['hour',
                'day_of_week',
                'day_of_year',
                'day_of_year',
                'month']:
            return False
        return True

    def run(self, results: Results) -> Tuple[float, pd.DataFrame]:
        # Prepare the data to contain only necessary columns and rows
        data = self._select(results)

        evaluation = self._process(data)
        summary = self._summarize(evaluation)

        self._plot(results, data, evaluation)

        return summary, evaluation


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

    def __iter__(self) -> Iterator:
        return iter(self._durations)

    def __len__(self) -> int:
        return len(self._durations)

    def __getitem__(self, key: str) -> float:
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
        self.system = system
        system_dir = system.configs['General']['data_dir']
        data_dir = os.path.join(system_dir, 'results')
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir, exist_ok=True)

        # noinspection PyProtectedMember
        self._database = deepcopy(system._database)
        self._database.dir = data_dir
        self._database.enabled = True
        self._datastore = pd.HDFStore(os.path.join(data_dir, 'results.h5'))

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

                io.write_csv(self.system, results_data, results_file)

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

    @property
    def name(self):
        return self.system.name
