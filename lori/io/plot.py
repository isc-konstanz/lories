# -*- coding: utf-8 -*-
"""
lori.io.plot
~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
from typing import List, Literal, Optional, Tuple

import matplotlib.pyplot as plt
import seaborn as sns

import numpy as np
import pandas as pd

logging.getLogger("PIL.PngImagePlugin").setLevel(logging.WARN)
logging.getLogger("matplotlib").setLevel(logging.WARN)
logger = logging.getLogger(__name__)

COLORS = [
    "#004F9E",
    "#FFB800",
]

INCH = 2.54
WIDTH = 32
HEIGHT = 9


# noinspection PyDefaultArgument, SpellCheckingInspection
def line(
    x: Optional[pd.Series | str] = None,
    y: Optional[pd.DataFrame | pd.Series | str] = None,
    data: Optional[pd.DataFrame] = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    xlim: Tuple[float, float] = None,
    ylim: Tuple[float, float] = None,
    grids: Optional[Literal["both", "x", "y"]] = None,
    colors: List[str] = COLORS,
    palette: Optional[str] = None,
    hue: Optional[str] = None,
    width: int = WIDTH,
    height: int = HEIGHT,
    show: bool = False,
    file: str = None,
    **kwargs,
) -> None:
    plt.figure(figsize=(width / INCH, height / INCH), dpi=120, tight_layout=True)

    color_num = max(len(np.unique(data[hue])) if hue else len(data.columns) - 1, 1)
    if color_num > 1:
        if palette is None:
            palette = f"blend:{','.join(colors)}"
        kwargs["palette"] = sns.color_palette(palette, n_colors=color_num)
        kwargs["hue"] = hue
    else:
        kwargs["color"] = colors[0]

    plot = sns.lineplot(
        x=x,
        y=y,
        data=data,
        errorbar=("pi", 50),
        estimator=np.median,
        **kwargs,
    )

    if isinstance(x, str) and x in ["hour", "horizon"]:
        index_unique = data[x].astype(int).unique()
        index_unique.sort()
        plt.xticks(index_unique, labels=index_unique)

    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)
    plt.box(on=False)

    if xlim is not None:
        plt.xlim(xlim)
    if ylim is not None:
        plt.ylim(ylim)

    if grids is not None:
        plt.grid(color="grey", linestyle="--", linewidth=0.25, alpha=0.5, axis=grids)

    if show:
        plt.show()
    if file is not None:
        plot.figure.savefig(file)

    plt.close(plot.figure)
    plt.clf()


# noinspection PyDefaultArgument, SpellCheckingInspection
def bar(
    x: Optional[pd.Index | pd.Series | str] = None,
    y: Optional[pd.DataFrame | pd.Series | str] = None,
    data: Optional[pd.DataFrame] = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    label_type: Optional[Literal["edge", "center"]] = None,
    colors: List[str] = COLORS,
    palette: Optional[str] = None,
    hue: Optional[str] = None,
    width: int = WIDTH,
    height: int = HEIGHT,
    show: bool = False,
    file: str = None,
    **kwargs,
) -> None:
    plt.figure(figsize=(width / INCH, height / INCH), dpi=120, tight_layout=True)

    color_num = max(len(np.unique(data[hue])) if hue else len(data.columns) - 1, 1)
    if color_num > 1:
        if palette is None:
            palette = f"blend:{','.join(colors)}"
        kwargs["palette"] = sns.color_palette(palette, n_colors=color_num)
        kwargs["hue"] = hue
    else:
        kwargs["color"] = colors[0]

    plot = sns.barplot(x=x, y=y, data=data, **kwargs)

    if (isinstance(x, str) and len(data[x]) > 24) or (
        isinstance(x, (pd.Index, pd.Series, np.ndarray)) and len(np.unique(x)) > 24
    ):
        plot.xaxis.set_tick_params(rotation=45)

    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)

    plt.box(on=False)

    if label_type is not None:
        plot.bar_label(plot.containers[0], label_type=label_type)

    if show:
        plt.show()
    if file is not None:
        plot.figure.savefig(file)

    plt.close(plot.figure)
    plt.clf()


# noinspection PyDefaultArgument, SpellCheckingInspection
def quartiles(
    x: Optional[pd.Series | str] = None,
    y: Optional[pd.DataFrame | pd.Series | str] = None,
    data: Optional[pd.DataFrame] = None,
    title: str = "",
    xlabel: str = "",
    ylabel: str = "",
    method: str = "bar",
    colors: List[str] = COLORS,
    palette: Optional[str] = None,
    hue: Optional[str] = None,
    width: int = WIDTH,
    height: int = HEIGHT,
    show: bool = False,
    file: str = None,
    **kwargs,
) -> None:
    plt.figure(figsize=(width / INCH, height / INCH), dpi=120, tight_layout=True)

    color_num = max(len(np.unique(data[hue])) if hue else len(data.columns) - 1, 1)
    if color_num > 1:
        if palette is None:
            palette = f"blend:{','.join(colors)}"
        kwargs["palette"] = sns.color_palette(palette, n_colors=color_num)
        kwargs["hue"] = hue
    else:
        kwargs["color"] = colors[0]

    if method in ["bar", "bars"]:
        fliers = dict(marker="o", markersize=3, markerfacecolor="none", markeredgecolor="lightgrey")
        plot = sns.boxplot(
            x=x,
            y=y,
            data=data,
            flierprops=fliers,
            **kwargs,
        )

        if (isinstance(x, str) and len(data[x]) > 24) or (isinstance(x, pd.Series) and len(np.unique(x)) > 24):
            plot.xaxis.set_tick_params(rotation=45)

    elif method == "line":
        # stats = data.groupby([x]).describe()
        # index_values = stats.index
        # index_unique = index_values.astype(int).unique().values
        # index_unique.sort()
        #
        # medians = stats[(y, '50%')]
        # quartile1 = stats[(y, '25%')]
        # quartile3 = stats[(y, '75%')]

        plot = sns.lineplot(
            x=x,
            y=y,
            data=data,
            errorbar=("pi", 50),
            estimator=np.median,
            **kwargs,
        )

        # plot.fill_between(index_values, quartile1, quartile3, color=color_palette[0], alpha=0.3)

        if isinstance(x, str) and x in ["hour", "horizon"]:
            index_unique = data[x].astype(int).unique()
            index_unique.sort()
            plot.set_xticks(index_unique, labels=index_unique)

    else:
        logger.error(f'Invalid boxplot method "{method}"')
        return

    plot.set(xlabel=xlabel, ylabel=ylabel, title=title)

    if show:
        plt.show()
    if file is not None:
        plot.figure.savefig(file)

    plt.close(plot.figure)
    plt.clf()


def histograms(data: pd.DataFrame, bins: int = 100, show: bool = False, path: str = "") -> None:
    for column in data.columns:
        plt.figure(figsize=(WIDTH, HEIGHT), dpi=120, tight_layout=True)

        # Create equal space bin values per column
        bin_data = []
        bin_domain = data[column].max() - data[column].min()
        bin_step = bin_domain / bins

        counter = data[column].min()
        for i in range(bins):
            bin_data.append(counter)
            counter = counter + bin_step

        # Add the last value of the counter
        bin_data.append(counter)

        bin_values, bin_data, patches = plt.hist(data[column], bins=bin_data)
        count_range = max(bin_values) - min(bin_values)
        sorted_values = list(bin_values)
        sorted_values.sort(reverse=True)

        # Scale plots by stepping through sorted bin_data
        for i in range(len(sorted_values) - 1):
            if abs(sorted_values[i] - sorted_values[i + 1]) / count_range < 0.80:
                continue
            else:
                plt.ylim([0, sorted_values[i + 1] + 10])
                break

        # Save histogram to appropriate folder
        path_dist = os.path.join(path, "dist")
        path_file = os.path.join(path_dist, "{}.png".format(column))
        if not os.path.isdir(path_dist):
            os.makedirs(path_dist, exist_ok=True)

        plt.title(r"Histogram of " + column)
        plt.savefig(path_file)

        if show:
            plt.show()
        plt.close()
        plt.clf()
