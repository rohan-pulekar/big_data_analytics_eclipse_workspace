package e63.course.assignment4;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * This program is for Assignment4 Problem4 of e63 course (Big Data Analytics)
 * This is the basic version, not the experience programmer version
 * 
 * @author rpulekar
 *
 */
public class Assignment4_Problem4_Basic {
	public static void main(String[] args) throws Exception {

		// set input and out files/dirs
		String inputFileName = args[0];
		String outputDirName = args[1];

		// Create a Java Spark Config.
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Assignment4_Problem4_Basic");

		// Create a Java Spark Context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// Load the input file.
		JavaRDD<String> inputFileContents = sparkContext.textFile(inputFileName);

		// Calculate full bigrams list
		JavaRDD<String> bigramsListRDD = inputFileContents.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) {
				return getBigrams(line);
			}
		});

		// Transform into map of bigrams and a default count of one
		JavaPairRDD<String, Integer> bigramsListWithDefaultCount = bigramsListRDD
				.mapToPair(new PairFunction<String, String, Integer>() {

					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String x) {
						return new Tuple2<String, Integer>(x, 1);
					}
				});

		// Call reduceByKey to count occurrence of each bigram
		JavaPairRDD<String, Integer> bigramsAndTheirCount = bigramsListWithDefaultCount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				});

		// Sort the bigrams list by bigram (which is the key)
		JavaPairRDD<String, Integer> sortedBigramsAndTheirCount = bigramsAndTheirCount.sortByKey();

		// Save the bigrams count list back to the text file, causing
		// evaluation.
		sortedBigramsAndTheirCount.saveAsTextFile(outputDirName);

		// close the spark context
		sparkContext.close();
	}

	public static List<String> getBigrams(String line) {

		// tokenize the line
		StringTokenizer tokenizer = new StringTokenizer(line);
		int numberOfWordsInSentence = tokenizer.countTokens();

		// if there are less than 2 tokens on the line then there is no bigram
		// on that line
		if (numberOfWordsInSentence < 2) {
			return new ArrayList<String>(0);
		}

		// construct list of words in the sentence
		List<String> wordsInSentence = new ArrayList<String>(numberOfWordsInSentence);
		while (tokenizer.hasMoreTokens()) {
			String token = tokenizer.nextToken();
			if (token != null && !token.isEmpty()) {
				wordsInSentence.add(token);
			}
		}

		List<String> bigrams = new ArrayList<String>(numberOfWordsInSentence - 1);

		// loop for creating bigrams
		for (int wordCounter = 0; wordCounter < numberOfWordsInSentence - 1; wordCounter++) {

			// get the first word of potential bigram
			String firstWord = wordsInSentence.get(wordCounter);

			// if first word ends with . or ? or ! then it is not part of a
			// bigram
			if (firstWord.endsWith(".") || firstWord.endsWith("?") || firstWord.endsWith("!")) {
				continue;
			}

			// clean first word by elimiating punctuations and convert it to
			// lower case
			firstWord = firstWord.replaceAll("[^A-Za-z0-9]", "").toLowerCase();

			// clean second word by elimiating punctuations and convert it to
			// lower case
			String secondWord = wordsInSentence.get(wordCounter + 1).replaceAll("[^A-Za-z0-9]", "").toLowerCase();

			// add current bigram to the list of bigrams if neither of them are
			// not empty
			if (!firstWord.isEmpty() && !secondWord.isEmpty()) {
				bigrams.add(firstWord + " " + secondWord);
			}
		}
		return bigrams;
	}
}
