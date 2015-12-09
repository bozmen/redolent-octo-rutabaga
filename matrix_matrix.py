from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def flatMapper( line ):
	tokens = line.split(' ')
	matrixName = str(tokens[0])
	row = int(tokens[1])
	column = int(tokens[2])
	value = int(tokens[3])
	if (matrixName == "A"):
		for i in range(0,5):
			yield((row, i), ("A", column, value))
	elif (matrixName == "B"):
		for i in range(0,5):
			yield((i, column),  ("B", row, value))

def mapper( k,v ):
	return(k, v)

def reducer( a, b ):
	print(str(a) + "   " + str(b) + "\n");
	sum = 0
	# for element in v[1]:
	# 	matrixName1 = element[0]
	# 	column1 = element[1]
	# 	value1 = element[2]
	# 	for other in v[1]:
	# 	 	matrixName2 = other[0]
	# 	 	column2 = other[1]
	# 	 	value2 = other[2]
	# 	 	if matrixName1 != matrixName2:
	# 	 		if column1 == column2:
	# 				sum += value1 + value2
	# return (v[0], sum)
	return a + b




if __name__ == '__main__':
	a = 0
	sc = SparkContext(appName="Matrix Vector Multiplication")
	matrixlines = sc.textFile("/home/ozzmen/Desktop/projects/cs425/5x5_5x5.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/output.txt", "w")

	result = matrixlines.flatMap(lambda line: flatMapper(line)) \
											.map(lambda k, v: mapper(k, v))

	output.write(str(result.collect()))
	result = result.aggregateByKey(reducer)

	output.write("\n")
	output.write(str(result.collect()))

	sc.stop()


