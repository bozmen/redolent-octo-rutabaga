from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def rowMapper( line ):
	return line[0].map(lambda value: (line[1], value))

if __name__ == '__main__':
	sc = SparkContext(appName="Matrix Vector Multiplication")
	matrixlines = sc.textFile("/home/ozzmen/Desktop/projects/cs425/datasetmatrix1", 1)
	vectorlines = sc.textFile("/home/ozzmen/Desktop/projects/cs425/datasetvector", 1)
	output = open("output.txt", "w")

	output.write(str(matrixlines.collect()))
	output.write('\n')
	matrixlines = matrixlines.zipWithIndex().map(lambda (v, i): (i, v.split(" ")))

	output.write(str(matrixlines.collect()))

	# elements = matrixlines.map(lambda line: rowMapper(line))

	# output.write(str(elements.count()))
	#matrix_lines_list = matrixlines.collect()
	#for (item) in matrix_lines_list:
	#	output.write(str(matrixlines.count()))
	#	output.write("\n")
	sc.stop()


