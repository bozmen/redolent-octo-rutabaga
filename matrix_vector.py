from __future__ import print_function

import sys
import subprocess
from operator import add

from pyspark import SparkContext



if __name__ == '__main__':
	start_time = time.time()
	a = 0
	sc = SparkContext(appName="Matrix Vector Multiplication")

	matrixlines1 = sc.textFile("/home/ozzmen/Desktop/projects/cs425/100x100_1.txt", 1)
	vectorlines2 = sc.textFile("/home/ozzmen/Desktop/projects/cs425/100x100_2.txt", 1)
	output = open("/home/ozzmen/Desktop/projects/cs425/output.txt", "w")



	elems1 = matrixlines1.flatMap(lambda line: flatMapper(line))
	elems2 = matrixlines2.flatMap(lambda line: flatMapper(line))

	elemPairs = elems1.join(elems2)

	result = elemPairs.reduceByKey(reducer)

	output.write("\n")
	output.write(str(result.collect()))
	elapsed_time = time.time() - start_time
	output.write("\n" + "annen")
	sc.stop()


