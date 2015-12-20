
from __future__ import print_function

import re
import sys
import time
from operator import add

from pyspark import SparkContext


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    current_milli_time = lambda: int(round(time.time() * 1000))
    start_time = current_milli_time()
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    print("""WARN: This is a naive implementation of PageRank and is
          given as an example! Please refer to PageRank implementation provided by graphx""",
          file=sys.stderr)

    # Initialize the spark context.
    sc = SparkContext(appName="PythonPageRank")
    output = open("/home/ozzmen/Desktop/projects/cs425/pagerank_output.txt", "w")


    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = sc.textFile(sys.argv[1], 1)
    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    iteration_start = current_milli_time()
    total_map_time = 0
    total_reduce_time = 0
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        map_start = time.time()
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        total_map_time += time.time() - map_start

        # Re-calculates URL ranks based on neighbor contributions.
        reduce_start = time.time()
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        total_reduce_time += time.time() - reduce_start
    iteration_finish = current_milli_time()

    finish_time = current_milli_time()
    output.write("\nTotal elapsed time: " + str(finish_time - start_time) + "ms")
    output.write("\nIteration time: " + str(iteration_finish - iteration_start) + "ms")
    output.write("\nAverage intteration time: " + str((iteration_finish - iteration_start)/int(sys.argv[2])) + "ms")
    output.write("\nMap time: " + str(round(total_map_time*1000)) + "ms")
    output.write("\nReduce time: " + str(round(total_reduce_time*1000)) + "ms")


    sc.stop()
