from __future__ import division
import numpy as np
import scipy.sparse as sp
import pandas as pd
import argparse
import sys

# This script takes a graph file, a teleportation rate, and a convergence
# threshold as arguments and it outputs the page rank of each 
# page to standard output 

parser = argparse.ArgumentParser(description='Calculate the page rank given a graph file.')
parser.add_argument('-teleportation_rate', '-t', nargs='?', default='0.2', type=float)
parser.add_argument('-epsilon', '-e', help='convergence threshold', 
                    nargs='?', default='1e-4', type=float)
parser.add_argument('input_file', type=argparse.FileType('r'))
parser.add_argument('output_file', nargs='?', default=sys.stdout, type=argparse.FileType('w'))

args = parser.parse_args()
epsilon = args.epsilon
teleportation_rate = args.teleportation_rate

# read the graph into a pandas data frame
web_graph = pd.read_csv(args.input_file, sep='\t', comment='#', header=None)


# store the original IDs and create new, consecutive integer IDs for each item
orig_id_to_matrix_id = {}
matrix_id_to_orig_id = []
for page in web_graph[0].values:
    if page not in orig_id_to_matrix_id:
        matrix_id_to_orig_id.append(page)
        orig_id_to_matrix_id[page] = len(matrix_id_to_orig_id) - 1
for page in web_graph[1].values:
    if page not in orig_id_to_matrix_id:
        matrix_id_to_orig_id.append(page)
        orig_id_to_matrix_id[page] = len(matrix_id_to_orig_id) - 1

# use pandas grouping features to count the number of outlinks from each node
# and calculate the transition probability for each outlink, i.e. 1 / num_links
web_graph['probs'] = 1.0 / web_graph.groupby([0])[1].transform('count')

# find the number of pages in the graph and create a sparse square matrix
# with dimension num_pages by num_pages (using the new IDs and the calculated 
# transition probabilities)
num_pages = len(matrix_id_to_orig_id)
M = sp.coo_matrix((web_graph.probs.values, 
                   ([orig_id_to_matrix_id[id] for id in web_graph[1].values], 
                    [orig_id_to_matrix_id[id] for id in web_graph[0].values])), 
                  shape=(num_pages, num_pages))

# initialize the page rank vector to a uniform probability distribution
v = np.ones((num_pages, 1)) / num_pages

# repeated multiply by the tranisiton matrix and then normalize the resulting
# vector, checking for convergence each time
v_prev = None
for i in xrange(300):
    v = (1 - teleportation_rate) * M.dot(v)
    v = v + (1 - np.sum(v)) / len(v)
    if v_prev is not None:
        sum_abs_diff = np.sum(np.abs(v_prev - v))
        if sum_abs_diff < epsilon:
            break
    v_prev = v

# output the pages and their corresponding ranks
for i, prob in enumerate(v):
    args.output_file.write('{0}\t{1}\n'.format(matrix_id_to_orig_id[i], float(prob)))