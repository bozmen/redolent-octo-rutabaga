from __future__ import print_function

import sys
import subprocess
from operator import add

from pyspark import SparkContext



if __name__ == '__main__':
	a = 0
	sc = SparkContext(appName="Matrix Vector Multiplication")

	matrixlines1 = sc.textFile("/home/ozzmen/Desktop/projects/cs425/100x100_1.txt", 1)
	vectorlines2 = sc.textFile("/home/ozzmen/Desktop/projects/cs425/100x100_2.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/output.txt", "w")



	elems1 = matrixlines1.flatMap(lambda line: flatMapper(line))
	elems2 = matrixlines2.flatMap(lambda line: flatMapper(line))

	output.write(str(elems1.collect()))
	output.write("\n")
	output.write(str(elems2.collect()))

	elemPairs = elems1.join(elems2)

	output.write("\n")
	# output.write(str(elemPairs.collect()))

	result = elemPairs.reduceByKey(reducer)

	output.write("\n")
	output.write(str(result.collect()))
	sc.stop()


