package e63.course.final_project;

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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import e63.course.dtos.MassachusettsHighway;
import e63.course.kafka_decoders.FloatDecoder;
import e63.course.kafka_decoders.HighwayDecoder;
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
public class HighwayInfoKafkaSlidingWIndowConsumer {

	// initialize the logger that is based on slf4j library
	private static final Logger LOGGER = LoggerFactory.getLogger(HighwayInfoKafkaSlidingWIndowConsumer.class);

	// date format for printing times in log output
	private static final DateFormat dateTimeFormat = new SimpleDateFormat("hh:mm:ss");

	private static int STREAMING_BATCH_DURATION_IN_SECS = 10;

	private static int WINDOW_DURATION_IN_SECS = 120;

	private static int SLIDE_INTERVAL_IN_SECS = 30;

	/**
	 * The main function for this class
	 * 
	 * @param args
	 *            (<brokers> <topics>)
	 */
	public static void main(String[] args) {

		// make sure 2 arguments are passed to the program
		if (args.length < 3) {
			System.err.println("Usage: HighwayInfoKafkaSlidingWIndowConsumer <kafkaBrokers> <topics> <zookeeper>\n"
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
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("HighwayInfoKafkaSlidingWIndowConsumer");
		// sparkConf.setMaster("local[5]"); // this is to run the program as a
		// standalone java application

		// create spark context from the spark configuration
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// create spark streaming context with batch interval of 1 second
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext,
				Durations.seconds(STREAMING_BATCH_DURATION_IN_SECS));

		// create a kafka topics set
		HashSet<String> kafkaTopicsSet = new HashSet<String>(Arrays.asList(kafkaTopic.split(",")));

		// create a map for kafka input DStream params
		HashMap<String, String> kafkaInputDStreamParams = new HashMap<String, String>();
		kafkaInputDStreamParams.put("metadata.broker.list", kafkaBrokers);
		kafkaInputDStreamParams.put("zookeeper.connect", zookeeper);
		kafkaInputDStreamParams.put("group.id", "HighwayInfoKafkaSlidingWIndowConsumer");

		System.out.println("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);
		LOGGER.info("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);

		// create a pair input DStream
		JavaPairInputDStream<MassachusettsHighway, Float> highwayAndSpeedsInputDStream = KafkaUtils.createDirectStream(
				streamingContext, MassachusettsHighway.class, Float.class, HighwayDecoder.class, FloatDecoder.class,
				kafkaInputDStreamParams, kafkaTopicsSet);

		// here reduceByKeyAndWindow is used to create a sliding windowed
		// stream.
		// the window duration is WINDOW_DURATION_IN_SECS seconds
		// and slide interval is SLIDE_INTERVAL_IN_SECS seconds
		JavaPairDStream<MassachusettsHighway, Float> highwayAndSpeedsDStream = highwayAndSpeedsInputDStream
				.window(Durations.seconds(WINDOW_DURATION_IN_SECS), Durations.seconds(SLIDE_INTERVAL_IN_SECS));

		// this function is to access each RDD
		VoidFunction<JavaPairRDD<MassachusettsHighway, Float>> functionToLogOutEachRDD = new VoidFunction<JavaPairRDD<MassachusettsHighway, Float>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<MassachusettsHighway, Float> highwayAndSpeedRDD) throws Exception {

				// some calendar and date operations to print time stamps for
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.SECOND, -1 * WINDOW_DURATION_IN_SECS);
				Date currentTimeStamp = new Date();
				Date timeStampAtStartOfWindow = calendar.getTime();
				System.out.println("");
				System.out.println("Current time: " + dateTimeFormat.format(currentTimeStamp)
						+ " showing highway and speed in last " + WINDOW_DURATION_IN_SECS + " secs (from "
						+ dateTimeFormat.format(timeStampAtStartOfWindow) + " to now)");

				JavaPairRDD<MassachusettsHighway, Float> sortedHighwayAndSpeedRDD = highwayAndSpeedRDD.sortByKey();

				// for each functinon to log out each tuple of number and count
				sortedHighwayAndSpeedRDD.foreach(functionToLogOutEachTuple);
			}

			// this function is to access each tuple in the RDD and then print
			// that tuple to the log wih LOGGER
			VoidFunction<Tuple2<MassachusettsHighway, Float>> functionToLogOutEachTuple = new VoidFunction<Tuple2<MassachusettsHighway, Float>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<MassachusettsHighway, Float> tuple) throws Exception {
					System.out.println("Highway:" + tuple._1() + " Speed:" + tuple._2());
				}
			};
		};

		// call functionToLogOutEachRDD on each RDD of numberAndCountPairs so
		// that the RDDs can be printed out in logs
		highwayAndSpeedsDStream.foreachRDD(functionToLogOutEachRDD);

		// Start the computation and await termination
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
