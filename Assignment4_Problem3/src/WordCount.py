'''
Created on Feb 23, 2016

@author: rpulekar  
This program is for Assignment4 Problem3 of e63 course (Big Data Analytics)
'''
from pyspark import SparkConf, SparkContext
import string
import sys

# Set the Spark configuration
conf = SparkConf().setAppName('Assignment4_Problem3')

# Set the spark context
sparkContext = SparkContext(conf = conf)

# set input/output filenames/dirs
input_file_name = sys.argv[1]
output_dir_name = sys.argv[2]

# Load the input data.
inputFileContents = sparkContext.textFile(input_file_name)

# Tokenize each line
words_list_RDD = inputFileContents.flatMap(lambda line: line.split())

# Convert tokens to lower-case & removes punctuation before creating a tuple (token, 1) for each word
words_list_with_default_count = words_list_RDD.map(lambda word: (str(word.lower()).translate(None,string.punctuation), 1))

# Filter the list to eliminate blanks
filtered_words_list = words_list_with_default_count.filter(lambda keyValue: len(keyValue[0])>0)

# Call reduceByKey to count occurrence of each word
words_and_their_count = filtered_words_list.reduceByKey(lambda a, b: a+b)

# Sort the list by word (which is the key)
sorted_words_and_their_count=words_and_their_count.sortByKey()

# Save the word count list back to the text file, causing evaluation.
sorted_words_and_their_count.saveAsTextFile(output_dir_name)