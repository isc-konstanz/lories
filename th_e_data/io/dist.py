# -*- coding: utf-8 -*-
"""
    th-e-sim.io.dist
    ~~~~~~~~~~~~~~~~~~~~~


"""
import os
import logging
import matplotlib.pyplot as plt

logging.getLogger('matplotlib').setLevel(logging.WARN)


def print_distributions(data, path=''):
    # Desired number of bins in each plot
    bin_num = 100
    for column in data.columns:  # create 100 equal space bin values per column.
        bins = []
        bin_domain = data[column].max() - data[column].min()
        bin_step = bin_domain / bin_num

        counter = data[column].min()
        for i in range(bin_num):
            bins.append(counter)
            counter = counter + bin_step

        # Add the last value of the counter
        bins.append(counter)

        bin_values, bins, patches = plt.hist(data[column], bins=bins)
        count_range = max(bin_values) - min(bin_values)
        sorted_values = list(bin_values)
        sorted_values.sort(reverse=True)

        # Scale plots by stepping through sorted bins
        for i in range(len(sorted_values) - 1):
            if abs(sorted_values[i] - sorted_values[i + 1]) / count_range < 0.80:
                continue
            else:
                plt.ylim([0, sorted_values[i + 1] + 10])
                break

        # Save histogram to appropriate folder
        path_dist = os.path.join(path, 'dist')
        path_file = os.path.join(path_dist, '{}.png'.format(column))
        if not os.path.isdir(path_dist):
            os.makedirs(path_dist, exist_ok=True)

        plt.title(r'Histogram of '+column)
        plt.savefig(path_file)
        plt.close()
        plt.clf()
