#!/usr/bin/env python3
import time
import argparse
import math
from pathlib import Path
import sys
from pyspark import SparkConf, SparkContext

def main(args):
    start_time = time.time()
    ################################### FUNCTIONS ###################################
    #################################################################################
    def parse_line(edge):
        '''Parse line input. If edge consists of one node expand it. map int to node pairs.
           Return -1 in case of comments
        '''
        if not edge.startswith('#'):
            nodes = edge.split()
            if len(nodes) == 1:
                nodes = 2 * nodes
            return tuple(map(int, nodes))
        return -1

    def agg_neighbors(x, y):
        '''Collect all neighbors of a given node to a list of unique nodes by extending the list of the first found neighbor
           each time we find a new one not already existing in the list
        '''
        x.extend([n for n in y if n not in x])
        return x

    def nodes_with_not_neighbors(line):
        '''For a given base node x, find all the nodes that are not connected with it and are > x to avoid duplicate edges
           since our graphs are not directed
        '''
        base_node, neighbors = line
        not_neighbors = [node for node in nodes.value if node not in neighbors and node > base_node]
        return base_node, not_neighbors
    
    def calculate_scores(nodes):
        '''Calculate score for each edge according to the score metric given in input args
           cn => Common neighbors
           jc => Jaccard Coefficient
           aa => Adamic/Adar
           If a wrong input is given, or None for score metric, cn is used
        '''
        node_1, node_2 = nodes
        neighbors_1, neighbors_2 = map(set, [nodes_with_neighbors_brd.value[node_1], nodes_with_neighbors_brd.value[node_2]])
        common_neighbors = neighbors_1.intersection(neighbors_2)
        if args.score_metric == 'jc':
            score = len(common_neighbors) / (len(neighbors_1) + len(neighbors_2) - len(common_neighbors))
        elif args.score_metric == 'aa':
            score = sum([1 / math.log(len(nodes_with_neighbors_brd.value[nd])) for nd in common_neighbors])
        else:
            score = len(common_neighbors)
        return (node_1, node_2), score
    #################################################################################
    #################################################################################

    # Set sparK context configuration 
    conf = SparkConf().setMaster(args.master).setAppName("DIT178")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Set input path
    cwd = Path().cwd().absolute()
    data_file = (cwd / Path(args.input)).as_uri()

    # Define input file to parse and filtering of the needed lines
    edges = sc.textFile(data_file)\
        .map(parse_line)\
        .filter(lambda x: x != -1)

    # Define and cache RDD with all nodes and their neighbors.
    nodes_with_neighbors = edges\
        .flatMap(lambda edge:[edge, edge[::-1]])\
        .map(lambda edge:[edge[0], [edge[1]]])\
        .reduceByKey(lambda x, y: agg_neighbors(x, y))\
        .cache()
    
    # Broadcast above RDD as dictionary (small size)
    nodes_with_neighbors_brd = sc.broadcast(nodes_with_neighbors.collectAsMap())
    print(f"Size of 'nodes_with_neighbors_brd' var: {sys.getsizeof(nodes_with_neighbors_brd.value) / 1024 **2:.2f}mb")
    # Broadcast nodes list
    nodes = sc.broadcast(list(nodes_with_neighbors_brd.value.keys()))
    print(f"Size of 'nodes' var: {sys.getsizeof(nodes.value) / 1024 **2:.2f}mb")

    # Define combination of not connected nodes with scores
    scored_edges = nodes_with_neighbors\
        .map(nodes_with_not_neighbors)\
        .flatMapValues(lambda x:x)\
        .map(calculate_scores)

    # Perform score calculation and extract k best
    selected_results = scored_edges\
        .takeOrdered(args.edges_number, key=lambda x: -x[-1])

    # Print top k scores
    score_var = f"score: {args.score_metric if args.score_metric in ['jc', 'aa'] else 'cn'}" 
    print(36 * '=' + '\n' + f" File: {args.input} ".center(36, '=') + '\n' + 36 * '=')
    print(f"{'node 1':^10} | {'node 2':^10} | {score_var:^10}")
    print(36 * '=')
    for result in selected_results:
        if args.score_metric in ['jc', 'aa']:
            print(f" {result[0][0]:<9} | {result[0][1]:<10} | {result[1]:<.3f}")
        else:
            print(f" {result[0][0]:<9} | {result[0][1]:<10} | {result[1]:<.0f}")

    # Close spark context
    sc.stop()

    ########################################################
    m, s = divmod(time.time() - start_time, 60)
    h, m = divmod(m, 60)
    print("Execution time: %d:%02d:%02d" % (h, m, s))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DIT178 - Project')
    parser.add_argument('--input', type=str, dest='input', help='File Input', default=None)
    parser.add_argument('--edges-num', type=int, dest='edges_number', help='Edges Number', default=10)
    parser.add_argument('--score-metric', type=str, dest='score_metric', help='Score Metric (cn, jc, aa)', default='cn')
    parser.add_argument('--master', type=str, dest='master', help='Spark master', default='local[*]')
    args = parser.parse_args()

    main(args)
