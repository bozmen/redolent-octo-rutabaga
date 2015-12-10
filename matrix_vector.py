from __future__ import print_function

import sys
import subprocess
from operator import add
import time

from pyspark import SparkContext

# Creates the matrices at your spark folder
# usage: 
# bin/spark-submit '/home/ozzmen/Desktop/projects/cs425/matrix_matrix.py' i j k 
# multiples random matrices that size of ixj and jxk


# def flatMapper( line ):
# 	tokens = line.split(' ')
# 	matrixName = str(tokens[0])
# 	row = int(tokens[1])
# 	column = int(tokens[2])
# 	value = int(tokens[3])
# 	if (matrixName == "A"):
# 		for r in range(0,int(j) + 1):
# 			yield((row, r), ("A", column, value))
# 	elif (matrixName == "B"):
# 		for r in range(0,int(j) + 1):
# 			yield((r, column),  ("B", row, value))

def matrixMapper( line ):
	tokens = line.split(' ')
	row = int(tokens[0])
	column = int(tokens[1])
	value = int(tokens[2])
	return column, (row, value)

def vectorMapper(line):
	tokens = line.split(' ')
	row  = int(tokens[0])
	value = int(tokens[2])
	return row, value

def pairsafMapper(key):
	print("\n\n\n\nkey: " + "annen" + " value: " + "value[0]")


if __name__ == '__main__':
	current_milli_time = lambda: int(round(time.time() * 1000))
	start_time = current_milli_time()

	sc = SparkContext(appName="Matrix Vector Multiplication")
	args = sys.argv
	i = args[1]
	j = args[2]
	# generates random matrices
	file_generate_start = current_milli_time()
	subprocess.call(["/home/ozzmen/Desktop/projects/cs425/matrix_vector_generator.out", i, j])
	file_generate_finish = current_milli_time()

	matrixlines = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + i + "x" + j + "_1.txt", 1)
	vectorlines = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + j + "x" + str(1) + "_2.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/matrix_vector_output.txt", "w")

	map_start = current_milli_time()
	matrix_elems = matrixlines.map(lambda line: matrixMapper(line))

	vector_elems = vectorlines.map(lambda line: vectorMapper(line))

	join_start = current_milli_time()
	elemPairs = matrix_elems.join(vector_elems)

	join_finish = current_milli_time()

	finalPairs = elemPairs.map(lambda x: (x[1][0][0], x[1][0][1]*x[1][1]))
	map_finish = current_milli_time()

	reduce_start = current_milli_time()
	result = finalPairs.reduceByKey(add)
	reduce_finish = current_milli_time()

	output.write("\n")
	elapsed_time = current_milli_time() - start_time
	output.write("\nTotal elapsed time: " + str(elapsed_time) + "ms")
	output.write("\nFile generation time: " + str(file_generate_finish - file_generate_start) + "ms")
	output.write("\nMap time: " + str(map_finish - map_start - (join_finish - join_start)) + "ms")
	output.write("\nJoin time: " + str(join_finish - join_start) + "ms")
	output.write("\nReduce time: " + str(reduce_finish - reduce_start) + "ms")
	sc.stop()
