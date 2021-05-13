#* ------------
#* This code is provided solely for the personal and private use of
#* students taking the CSC367 course at the University of Toronto.
#* Copying for purposes other than this use is expressly prohibited.
#* All forms of distribution of this code, whether as given or with
#* any changes, are expressly prohibited.
#*
#* Authors: Bogdan Simion, Maryam Dehnavi, Felipe de Azevedo Piovezan
#*
#* All of the files in this directory and all subdirectories are:
#* Copyright (c) 2020 Bogdan Simion and Maryam Dehnavi
#* -------------

#This script relies HEAVILY on the output provided by the starter_code.
#If you changed it somehow or added new printf statements (neither of which you
#should do for the final submission), the script won't work.

import subprocess
import os
import re
from collections import defaultdict
from textwrap import wrap
import pickle
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import plotter

#executes a command and kills the python execution if
#said command fails.
def execute_command(command):
  print(">>>>> Executing command {}".format(command))
  process = subprocess.Popen(command, stdout = subprocess.PIPE,
      stderr = subprocess.STDOUT, shell=True,
      universal_newlines = True)
  return_code = process.wait()
  output = process.stdout.read()

  if return_code == 1:
    print("failed to execute command = ", command)
    print(output)
    exit()

  return output


def parse_perf(x):
  dict = {}
  dict['dump'] = x
  x = x.replace(',', '')
  # perf has this weird behavior where if we pass some counters with
  # "u", i.e., -e instructions:u,L1-dcache-loads:u
  # some of them will be reported without the u, which is
  # why it is removed here.
  items = {'seconds time elapsed' : 'time', #this is always recorded, but we're not using it.
       'instructions:u' : 'instructions',
       'L1-dcache-loads' : 'l1d_load',
       'L1-dcache-load-misses' : 'l1d_loadmisses',
       'LLC-loads' : 'll_load',
       'LLC-load-misses' : 'll_loadmisses',
       }
  #this     \/ whitespace is important to get the longest match.
  fp_regex = ('.*\s((\d+\.\d*)|(\d+))\s*{}\s*(#.*L1-dcache hits)?\s*' +
      re.escape('( +-') + '\s*(\d+\.\d+)' + re.escape('% )') + '.*')
  for name, key in items.items():
    vals = re.match(fp_regex.format(name), x, flags=re.DOTALL)
    if vals != None:
      dict[key] = vals[1]
  return dict

threads = [1,2,4,8]

#data 0, 1, 2


colours = {"nested-loop" : 'r',
           "sort-merge": 'b',
           "hash" : 'g',
           "sharded_columns row major" : 'c',
           "work queue" : 'm',
           1 : 'r',
           2 : 'b',
           3 : 'g',
           4 : 'm',
           8 : 'k',
           "symmetric partitioning, sort-merge": 'r',
           "symmetric partitioning, hash" : 'b',
           "fragment-and-replicate, sort-merge": 'g',
           "fragment-and-replicate, hash" : 'c',
           }


#keys are tuples:
#(filter, method, numthreads, [chunk_size])
results = defaultdict()
#this will create a file called "data.pickle". If this
#exists when the program is run, perf will not be run again
#on the results that were saved in said file. To force
#a fresh run, delete data.pickle before running the script.
if os.path.isfile('data.pickle'):
  with open('data.pickle', 'rb') as f:
    results = pickle.load(f)

#This function runs perf using the built in square image by default.
def run_perf_exp4(filter, method, numthreads = 1, chunk_size = 1, width = 1, repeat =
10):
    key = (filter, method, numthreads, chunk_size, width)
    if results.get(key) != None:
        return

    #get time
    main_args = './join-omp -h -r -t 8 ..data/dataset4'

    time = 0
    for i in range(repeat):
        ret = execute_command(main_args)
        time += float(ret[:])
        #print (ret)
    time = time / repeat
    return time


#Plot "mode" on the y axis and  number of threads on the x axis.
# Experiment 1: different join methods for the sequential implementations for datasets 0 to 2.
def exper1():
    # local_results = defaultdict(list)
    # for method in seq_methods:
    #     key = (method, nthread)
    #     local_results[method] += run_perf(*key)
    methods = {
        "nested-loop" : 1,
        "sort-merge": 2,
        "hash" : 3,
    }

    exper1_results = {
        "nested-loop" : [0.146360, 5.850614, 576.395439],
        "sort-merge": [0.007481, 0.015337, 0.268850],
        "hash" : [0.059637, 0.363222, 1.807843],
    }

    title = ('Different sequential join methods for datasets 0 to 2. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = list(methods.keys())

    xvals = [0,1,2]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper1_results[method] for method in methods_as_list]
    filename = 'exper1_1.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')

# For datasets 0-5, use graphs to compare the running time of the following pairs of parallel and partial join methods:

# fragment-and-replicate, hash join

def exper2_1():
    exper2_results = {
        "sort-merge": [0.162591, 0.179501, 0.253028, 24.786393, 56.136769, 54.139292],
        "hash" : [0.313449, 0.435347, 4.382361, 283.633887, 1845.083490, 2490.947151],
    }

    methods = {
        "sort-merge": 2,
        "hash" : 3,
    }

    title = ('fragment-and-replicate for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = list(methods.keys())

    xvals = [0,1,2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper2_results[method] for method in methods_as_list]
    filename = 'exper2_2.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')

def exper2_2():
    exper2_results = {
        "sort-merge": [0.170368, 0.163539, 0.329715, 23.684499, 67.771266, 37.147965],
        "hash" : [0.333051, 0.405987, 0.633864, 229.586091, 285.615256, 339.348297],
    }

    methods = {
        "sort-merge": 2,
        "hash" : 3,
    }

    title = ('symmetric partitioning for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = list(methods.keys())

    xvals = [0,1,2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper2_results[method] for method in methods_as_list]
    filename = 'exper2_2.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')


def exper2_3():
    exper2_results = {
        "symmetric partitioning, sort-merge": [0.170368, 0.163539, 0.329715, 23.684499, 67.771266, 37.147965],
        "symmetric partitioning, hash" : [0.333051, 0.405987, 0.633864, 229.586091, 285.615256, 339.348297],
        "fragment-and-replicate, sort-merge": [0.162591, 0.179501, 0.253028, 24.786393, 56.136769, 54.139292],
        "fragment-and-replicate, hash" : [0.313449, 0.435347, 4.382361, 283.633887, 1845.083490, 2490.947151],
    }

    methods = {
        "symmetric partitioning, sort-merge": 1,
        "symmetric partitioning, hash" : 2,
        "fragment-and-replicate, sort-merge": 3,
        "fragment-and-replicate, hash" : 4,
    }

    title = ('Pairs of parallel and partial join methods for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = list(methods.keys())

    xvals = [0,1,2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper2_results[method] for method in methods_as_list]
    filename = 'exper2_3.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')


def exper3_1():
    exper3_results = {
        1 : [0.072702, 0.365744, 1.560464, 455.683425, 576.657628, 657.563756],
        2 : [0.118600, 0.417370, 2.006000, 370.584751, 701.763510, 891.083446],
        4 : [0.200382, 0.408893, 2.771061, 305.424984, 1062.588166, 1398.033139],
        8 : [0.313449, 0.435347, 4.382361, 283.633887, 1845.083490, 2490.947151],
    }

    title = ('fragment-and-replicate, hash join for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = threads

    xvals = [0, 1, 2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper3_results[method] for method in methods_as_list]
    filename = 'exper3_1.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')

def exper3_2():
    exper3_results = {
        1 : [0.020897, 0.024156, 0.301492, 81.578643, 111.298650, 100.778580],
        2 : [0.078287, 0.075529, 0.227114, 48.605713, 70.574506, 66.011069],
        4 : [0.169541, 0.190730, 0.244118, 32.552181, 57.725792, 56.936766],
        8 : [0.211066, 0.224072, 0.305123, 25.105629, 56.594753, 54.086202],
    }

    title = ('fragment-and-replicate, sort-merge join for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = threads

    xvals = [0, 1, 2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper3_results[method] for method in methods_as_list]
    filename = 'exper3_2.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')

def exper3_3():
    exper3_results = {
        1 : [0.073598, 0.389940, 1.775914, 432.540894, 585.730640, 626.476245],
        2 : [0.142638, 0.431690, 1.395176, 332.062180, 463.557084, 482.925678],
        4 : [0.173001, 0.394243, 0.889258, 264.173367, 366.158240, 397.367687],
        8 : [0.333051, 0.405987, 0.633864, 254.623087, 299.633613, 355.931887],
    }

    title = ('symmetric partitioning, hash join for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = threads

    xvals = [0, 1, 2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper3_results[method] for method in methods_as_list]
    filename = 'exper3_3.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')

def exper3_4():
    exper3_results = {
        1 : [0.021867, 0.026246, 0.408210, 102.318398, 188.654621, 134.422899],
        2 : [0.076714, 0.075990, 0.369663, 60.292068, 145.573293, 86.938209],
        4 : [0.105480, 0.173720, 0.330839, 42.688311, 112.839942, 60.632642],
        8 : [0.226018, 0.221241, 0.395820, 38.813445, 99.199473, 53.292398],
    }

    title = ('symmetric partitioning, sort-merge join for datasets 0-5. Average over 10 runs.'.format(filter))
    ylabel = 'time(ms)'

    #sets are unordered by default, so impose an order with this list.
    methods_as_list = threads

    xvals = [0, 1, 2, 3, 4, 5]
    list_of_colors = [colours[method] for method in methods_as_list]
    list_of_yvals = [exper3_results[method] for method in methods_as_list]
    filename = 'exper3_4.png'
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
    #plt.xscale('log')
    plt.savefig(filename, bbox_inches='tight')


exper1()
exper2_1()
exper2_2()
exper2_3()
exper3_1()
exper3_2()
exper3_3()
exper3_4()