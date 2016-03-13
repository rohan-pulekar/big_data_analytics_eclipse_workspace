package e63.course.assignment4;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/*
 * @Author Rohan Pulekar
 * This class is for Assignment4 Problem1 of e63 course (Big Data Analytics)
 * Problem statement:
 * Problem 1.  Write a working WordCount program using Spark Java API that reads a file, e.g. Ulysis/4300.txt from an HDFS directory and writes the results of your calculations to an HDFS file. To improve your word count, remove any punctuation that might have attached itself to your words. Also transform all words into lower case so that the capitalization does not affect the word count. The original code used in lecture notes is provided in the attached mini-example-java.tar file. That archive also contains Maven’s pom.xml file. Run your program and demonstrate that it works. Submit working code inside the customary MS Word Document. Describe steps in your program.
 */
public class WordCount {
	public static void main(String[] args) throws Exception {

		// set input and out files/dirs
		String inputFileName = args[0];
		String outputDirName = args[1];

		// Create a Java Spark Configuration
		SparkConf sparkConf = new SparkConf().setAppName("Assignment4_Problem1");

		// Create a Java Spark Context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// Load the input data.
		JavaRDD<String> inputFileContents = sparkContext.textFile(inputFileName);

		// Split the input data into words
		JavaRDD<String> wordsListRDD = inputFileContents.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) {
				return Arrays.asList(line.split(" "));
			}
		});

		// Transform into a map of word and default count of 1
		JavaPairRDD<String, Integer> wordsListWithDefaultCount = wordsListRDD
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String word) {
						// the below is done to eliminate punctuations and to
						// convert the words into lower case
						word = word.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
						return new Tuple2<String, Integer>(word, 1);
					}
				});

		// Filter the list to eliminate blanks
		JavaPairRDD<String, Integer> filteredWordsList = wordsListWithDefaultCount
				.filter((Tuple2<String, Integer> tuple) -> !tuple._1.isEmpty());

		// Call reduceByKey to count occurrence of each word
		JavaPairRDD<String, Integer> wordsAndTheirCount = filteredWordsList
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		// Sort the list by word (which is the key)
		JavaPairRDD<String, Integer> sortedWordsAndTheirCount = wordsAndTheirCount.sortByKey();

		// Save the word count list back to the text file, causing evaluation.
		sortedWordsAndTheirCount.saveAsTextFile(outputDirName);

		// close the spark context
		sparkContext.close();
	}
}
