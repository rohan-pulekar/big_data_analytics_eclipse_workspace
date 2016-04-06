package e63.kafka.streaming;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * This class is for Assignment8 Problem3 of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * This class needs 2 parameters for execution. The way to execute this class
 * is: Problem3_Consumer <brokers> <topics> <zookeeper>
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem3_Consumer {

	// initialize the logger that is based on slf4j library
	private static final Logger LOGGER = LoggerFactory.getLogger(Problem3_Consumer.class);

	// date format for printing times in log output
	private static final DateFormat dateTimeFormat = new SimpleDateFormat("hh:mm:ss");

	/**
	 * The main function for this class
	 * 
	 * @param args
	 *            (<brokers> <topics>)
	 */
	public static void main(String[] args) {

		// make sure 2 arguments are passed to the program
		if (args.length < 3) {
			System.err.println("Usage: Problem3_Consumer <kafkaBrokers> <topics> <zookeeper>\n"
					+ " <brokers> is a list of one or more Kafka brokers e.g. localhost:9092\n"
					+ " <topics> is a list of one or more kafka topics to consume from e.g. assignment8_problem2and3_topic\n"
					+ " <zookeeper> is the IP and port on which server is running e.g. localhost:2181\n");
			System.exit(1);
		}

		// for my local instance this is "localhost:9092";
		String kafkaBrokers = args[0];

		// for my local instance this is assignment8_problem2and3_topic
		String kafkaTopic = args[1];

		// for my local instance this is localhost:2181
		String zookeeper = args[2];

		// Create a Java Spark Config.
		SparkConf sparkConf = new SparkConf().setAppName("Assignment8_Problem3_Consumer");
		// sparkConf.setMaster("local[5]"); // this is to run the program as a
		// standalone java application

		// create spark context from the spark configuration
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// create spark streaming context with batch interval of 1 second
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));

		// create a kafka topics set
		HashSet<String> kafkaTopicsSet = new HashSet<String>(Arrays.asList(kafkaTopic.split(",")));

		// create a map for kafka input DStream params
		HashMap<String, String> kafkaInputDStreamParams = new HashMap<String, String>();
		kafkaInputDStreamParams.put("metadata.broker.list", kafkaBrokers);
		kafkaInputDStreamParams.put("zookeeper.connect", zookeeper);
		kafkaInputDStreamParams.put("group.id", "Assignment8_Problem3_Consumer");

		System.out.println("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);
		LOGGER.info("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);

		// create a pair input DStream
		JavaPairInputDStream<String, String> pairInputDStream = KafkaUtils.createDirectStream(streamingContext,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaInputDStreamParams,
				kafkaTopicsSet);

		// create a stream of numbers received as messages
		JavaDStream<String> numbersReceivedAsMessages = pairInputDStream
				.map(new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		// create a pair DStream of numbers received as messages and their
		// count.
		// where the number received (in the form of string) will be key
		// and the count of number will be the value
		JavaPairDStream<String, Integer> numberAndCountAsOnePairs = numbersReceivedAsMessages
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		// create a reduce function that will add the counts of numbers received
		Function2<Integer, Integer, Integer> reduceFunction = new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1 + i2;
			}
		};

		// here reduceByKeyAndWindow is used to create a sliding windowed
		// stream.
		// the window duration is 30 seconds
		// and slide interval is 5 seconds
		JavaPairDStream<String, Integer> numberAndCountPairs = numberAndCountAsOnePairs
				.reduceByKeyAndWindow(reduceFunction, Durations.seconds(30), Durations.seconds(5));

		// this function is to access each RDD
		VoidFunction<JavaPairRDD<String, Integer>> functionToLogOutEachRDD = new VoidFunction<JavaPairRDD<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> javaPairRDD) throws Exception {

				// some calendar and date operations to print time stamps for
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.SECOND, -30);
				Date currentTimeStamp = new Date();
				Date timeStampOf30SecondsBefore = calendar.getTime();
				LOGGER.info("");
				LOGGER.info("Current time: " + dateTimeFormat.format(currentTimeStamp)
						+ " showing count of numbers in last 30 secs (from "
						+ dateTimeFormat.format(timeStampOf30SecondsBefore) + " to now)");

				// for each functinon to log out each tuple of number and count
				javaPairRDD.foreach(functionToLogOutEachTuple);
			}

			// this function is to access each tuple in the RDD and then print
			// that tuple to the log wih LOGGER
			VoidFunction<Tuple2<String, Integer>> functionToLogOutEachTuple = new VoidFunction<Tuple2<String, Integer>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<String, Integer> tuple) throws Exception {
					LOGGER.info("Number:" + tuple._1() + " count:" + tuple._2());
				}
			};
		};

		// call functionToLogOutEachRDD on each RDD of numberAndCountPairs so
		// that the RDDs can be printed out in logs
		numberAndCountPairs.foreachRDD(functionToLogOutEachRDD);

		// Start the computation and await termination
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
