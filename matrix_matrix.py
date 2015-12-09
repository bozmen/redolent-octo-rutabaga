from __future__ import print_function

import sys
import subprocess
from operator import add

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
		for r in range(0,int(j) + 1):
			yield((row, r), ("A", column, value))
	elif (matrixName == "B"):
		for r in range(0,int(j) + 1):
			yield((r, column),  ("B", row, value))

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

	print("a: " + str(a) + " b: "+ str(b) + "\n");
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


j = 0

if __name__ == '__main__':
	a = 0
	sc = SparkContext(appName="Matrix Vector Multiplication")

	args = sys.argv
	i = args[1]
	j = args[2]
	k = args[3]
	# generates random matrices
	subprocess.call(["/home/ozzmen/Desktop/projects/cs425/a.out", i, j, k])

	matrixlines1 = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + i + "x" + j + "_1.txt", 1)
	matrixlines2 = sc.textFile("/home/ozzmen/Downloads/spark-1.5.2/" + j + "x" + k + "_2.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/output.txt", "w")



	elems1 = matrixlines1.flatMap(lambda line: flatMapper(line))
	elems2 = matrixlines2.flatMap(lambda line: flatMapper(line))

	output.write(str(elems1.collect()))
	output.write("\n")
	output.write(str(elems2.collect()))

	elemPairs = elems1.join(elems2)

	result = elemPairs.reduceByKey(reducer)

	output.write("\n")
	output.write(str(result.collect()))
	sc.stop()


