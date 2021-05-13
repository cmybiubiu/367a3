import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import os.path
import time
import plotter

threads = [1,2,4,8]
seq_methods = {
    "nested-loop" : 1,
    "sort-merge": 2,
    "hash-join" : 3,
}
#data 0, 1, 2
seq_results = {
    "nested-loop" : [0.146360, 5.850614, 576.395439],
    "sort-merge": [0.007481, 0.015337, 0.268850],
    "hash-join" : [0.059637, 0.371360, 1.833634],
}
omp_methods = {
    "nested-loop" : 1,
    "sort-merge": 2,
    "hash join" : 3,
    "sharded_columns row major" : 4,
    "work queue" : 5,
}

colours = {"nested-loop" : 'r',
           "sort-merge": 'b',
           "hash-join" : 'g',
           "sharded_columns row major" : 'c',
           "work queue" : 'm',
           1 : 'r',
           2 : 'b',
           3 : 'g',
           4 : 'm',
           8 : 'k',
           }


def main():

    title = ('Different sequential join methods for datasets 0 to 2. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = list(seq_methods.keys())

    xvals = [0,1,2]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [seq_results[method] for method in methods_as_list]
    filename = 'graph_1.png'
    xlabel = "dataset"

    legends = []
    for i in range(len(methods_as_list)):
        patch = mpatches.Patch(color=list_of_colors[i], label=methods_as_list[i])
        legends += [patch]

    plt.clf()
    for i in range(len(list_of_yvals)):
        plt.plot(xvals, list_of_yvals[i], list_of_colors[i])

    plt.xticks(xvals, xvals)
    plt.legend(handles=legends)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.subplots_adjust(top=0.85)
    plt.title(wrap(title, 60), y = 1.08)
    plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')


if __name__ == '__main__':
    main()
