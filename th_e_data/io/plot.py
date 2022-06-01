# -*- coding: utf-8 -*-
"""
    th-e-sim.io.plot
    ~~~~~~~~~~~~~~~~~~~~~


"""
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns

logging.getLogger('PIL.PngImagePlugin').setLevel(logging.WARN)
logging.getLogger('matplotlib').setLevel(logging.WARN)
logger = logging.getLogger(__name__)


def print_lineplot(system, data, index, column, file, label='', title='', colors=None, **kwargs):
    plt.figure()
    if colors is None:
        color_num = index.nunique()
        colors = sns.dark_palette('#004F9E', n_colors=color_num, reverse=True)
    plot = sns.lineplot(x=index, y=column,
                        ci='sd',
                        data=data,
                        palette=colors,
                        **kwargs)
    plot.set(xlabel=label, ylabel='Power [W]', title=title)
    plt.show(block=False)
    fig = plot.figure

    plot_file = os.path.join(system.configs['General']['data_dir'], file + '.png')
    plot_dir = os.path.dirname(plot_file)

    if not os.path.isdir(plot_dir):
        os.makedirs(plot_dir, exist_ok=True)

    fig.savefig(plot_file)
    plt.close(fig)


def print_boxplot(system, data, index, column, file, label='', title='', colors=None, **kwargs):
    plt.figure()
    plot_fliers = dict(marker='o', markersize=3, markerfacecolor='none', markeredgecolor='lightgrey')
    plot_colors = colors if colors is not None else index.nunique()
    plot_palette = sns.light_palette('#004F9E', n_colors=plot_colors, reverse=True)
    plot = sns.boxplot(x=index, y=column,
                       data=data,
                       palette=plot_palette,
                       flierprops=plot_fliers,
                       # showfliers=False,
                       **kwargs)
    plot.set(xlabel=label, ylabel='Error [W]', title=title)
    plt.show(block=False)
    fig = plot.figure

    plot_file = os.path.join(system.configs['General']['data_dir'], file + '.png')
    plot_dir = os.path.dirname(plot_file)

    if not os.path.isdir(plot_dir):
        os.makedirs(plot_dir, exist_ok=True)

    fig.savefig(plot_file)
    plt.close(fig)
