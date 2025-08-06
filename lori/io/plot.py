# -*- coding: utf-8 -*-
"""
lori.io.plot
~~~~~~~~~~~~


"""

from __future__ import annotations

import logging
import os
from functools import wraps
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


def plot(func):
    @wraps(func)
    def wrapper(
        *args,
        width: int = WIDTH,
        height: int = HEIGHT,
        show: bool = False,
        file: Optional[str] = None,
        await_safe: bool = False,
        **kwargs,
    ) -> Optional[Tuple[plt.Figure, callable]]:
        plt.figure(figsize=(width / INCH, height / INCH), dpi=120, tight_layout=True)

        fig = func(*args, **kwargs)

        def safe_and_close():
            if show:
                plt.show()

            if file is not None:
                fig.savefig(file)

                plt.close(fig)
                plt.clf()

        if await_safe:
            return fig, safe_and_close
        else:
            safe_and_close()
            return

    return wrapper


# noinspection PyDefaultArgument, SpellCheckingInspection
@plot
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
    errorbar: Literal["pi", "se", "sd"] | Tuple[str, int] = None,
    estimator: callable = None,
    **kwargs,
) -> plt.Figure:
    color_num = max(len(np.unique(data[hue])) if hue else len(data.columns) - 1, 1)
    if color_num > 1:
        if palette is None:
            palette = f"blend:{','.join(colors)}"
        kwargs["palette"] = sns.color_palette(palette, n_colors=color_num)
        kwargs["hue"] = hue
    else:
        kwargs["color"] = colors[0]

    kwargs["errorbar"] = errorbar
    kwargs["estimator"] = estimator

    plot = sns.lineplot(
        x=x,
        y=y,
        data=data,
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

    return plot.figure


# noinspection PyDefaultArgument, SpellCheckingInspection
@plot
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
    **kwargs,
) -> plt.Figure:
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

    return plot.figure


# noinspection PyDefaultArgument, SpellCheckingInspection
# TODO: also wrap this function with @plot decorator
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


# TODO: also wrap this function with @plot decorator
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


# noinspection PyDefaultArgument, SpellCheckingInspection
@plot
def ridge(
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
    **kwargs,
) -> plt.Figure:
    sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})

    plot = sns.FacetGrid(data, row=hue, hue=hue, aspect=15, height=0.5, palette=palette)

    # Draw the densities in a few steps
    plot.map(sns.kdeplot, y, bw_adjust=0.5, clip_on=False, fill=True, alpha=1, linewidth=1.5)
    plot.map(sns.kdeplot, y, clip_on=False, color="w", lw=2, bw_adjust=0.5)

    # passing color=None to refline() uses the hue mapping
    plot.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)

    # Define and use a simple function to label the plot in axes coordinates
    def label(x, color, label_text):
        ax = plt.gca()
        ax.text(0, 0.2, label_text, fontweight="bold", color=color, ha="left", va="center", transform=ax.transAxes)

    # plot.map(label, y)

    # Set the subplots to overlap
    plot.figure.subplots_adjust(hspace=-0.25)

    # Remove axes details that don't play well with overlap
    plot.set_titles("")
    plot.set(yticks=[], ylabel="")
    plot.despine(bottom=True, left=True)

    return plot.figure
