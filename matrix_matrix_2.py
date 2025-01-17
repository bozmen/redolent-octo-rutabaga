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


def flatMapper( line ):
	tokens = line.split(' ')
	matrixName = str(tokens[0])
	row = int(tokens[1])
	column = int(tokens[2])
	value = int(tokens[3])
	if (matrixName == "A"):
		for r in range(0,int(i)/group_size + 1):
			yield((row/group_size, r), ("A", row, column, value))
	elif (matrixName == "B"):
		for r in range(0,int(i)/group_size + 1):
			yield((r, column/group_size),  ("B", row, column, value))

def mapper( k,v ):
	return(k, v)

def reducer( a, b ):
	if isinstance(a, tuple):
		first = a[0]
		matrixName1 = first[0]
		column1 = first[1]
		value1 = first[2]

		second = a[1]
		matrixName2 = second[0]
		column2 = second[1]
		value2 = second[2]

		if matrixName1 != matrixName2:
			if column1 == column2:
		 		return value1 * value2
			else:
				return 0
	first = b[0]
	matrixName1 = first[0]
	column1 = first[1]
	value1 = first[2]

	second = b[1]
	matrixName2 = second[0]
	column2 = second[1]
	value2 = second[2]

	if matrixName1 != matrixName2:
		if column1 == column2:
	 		return a + value1 * value2
		else:
			return a


if __name__ == '__main__':
	current_milli_time = lambda: int(round(time.time() * 1000))
	start_time = current_milli_time()

	group_size = 5

	sc = SparkContext(appName="Matrix Vector Multiplication")
	args = sys.argv
	i = args[1]
	j = args[2]
	k = args[3]
	# generates random matrices
	file_generate_start = current_milli_time()
	subprocess.call(["/home/ozzmen/Desktop/projects/cs425/matrix_matrix_generator.out", i, j, k])
	file_generate_finish = current_milli_time()

	matrixlines1 = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + i + "x" + j + "_1.txt", 1)
	matrixlines2 = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + j + "x" + k + "_2.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/matrix_matrix_output.txt", "w")


	map_start = current_milli_time()
	elems1 = matrixlines1.flatMap(lambda line: flatMapper(line))
	elems2 = matrixlines2.flatMap(lambda line: flatMapper(line))
	map_finish = current_milli_time()

	join_start = current_milli_time()
	elemPairs = elems1.join(elems2)
	join_finish = current_milli_time()

	reduce_start = current_milli_time()
	result = elemPairs.reduceByKey(reducer)
	reduce_finish = current_milli_time()

	output.write("\n")
	elapsed_time = current_milli_time() - start_time
	output.write("\nTotal elapsed time: " + str(elapsed_time) + "ms")
	output.write("\nFile generation time: " + str(file_generate_finish - file_generate_start) + "ms")
	output.write("\nMap time: " + str(map_finish - map_start) + "ms")
	output.write("\nJoin time: " + str(join_finish - join_start) + "ms")
	output.write("\nReduce time: " + str(reduce_finish - reduce_start) + "ms")
	sc.stop()


