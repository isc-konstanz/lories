# -*- coding: utf-8 -*-
"""
    th-e-sim.io.plot
    ~~~~~~~~~~~~~~~~~~~~~


"""
import os
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

logging.getLogger('PIL.PngImagePlugin').setLevel(logging.WARN)
logging.getLogger('matplotlib').setLevel(logging.WARN)
logger = logging.getLogger(__name__)

INCH = 2.54
WIDTH = 32/INCH
HEIGHT = 9/INCH


def print_lineplot(data, index, column, file,
                   title='', xlabel='', ylabel='Error [W]', color='#004F9E', **kwargs) -> None:
    plt.figure(figsize=[WIDTH, HEIGHT], dpi=120, tight_layout=True)
    color_num = max(len(data.columns) - 1, 1)
    colors = sns.dark_palette(color, n_colors=color_num, reverse=True)
    plot = sns.lineplot(x=index, y=column, ci='sd',
                        data=data,
                        palette=colors,
                        **kwargs)
    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)
    fig = plot.figure
    fig.savefig(file)
    plt.close(fig)


def print_barplot(data, index, column, file,
                  title='', xlabel='', ylabel='Error [W]', color='#004F9E', **kwargs) -> None:
    plt.figure(figsize=[WIDTH, HEIGHT], dpi=120, tight_layout=True)
    color_num = max(len(data.columns) - 1, 1)
    colors = sns.dark_palette(color, n_colors=color_num, reverse=True)
    plot = sns.barplot(x=index, y=column, ci='sd',
                       data=data,
                        palette=colors,
                       **kwargs)
    if len(np.unique(index)) > 24:
        plot.xaxis.set_tick_params(rotation=45)
    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)
    fig = plot.figure
    fig.savefig(file)
    plt.close(fig)


def print_boxplot(data, index, column, file,
                  title='', xlabel='', ylabel='Error [W]', color='#004F9E', **kwargs) -> None:
    plt.figure(figsize=[WIDTH, HEIGHT], dpi=120, tight_layout=True)
    fliers = dict(marker='o', markersize=3, markerfacecolor='none', markeredgecolor='lightgrey')
    color_num = max(len(data.columns) - 1, 1)
    colors = sns.light_palette(color, n_colors=color_num, reverse=True)
    plot = sns.boxplot(x=index, y=column,
                       data=data,
                       palette=colors,
                       flierprops=fliers,  # showfliers=False,
                       **kwargs)
    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)
    fig = plot.figure
    fig.savefig(file)
    plt.close(fig)
