package e63.course.streaming.consumer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import e63.course.dtos.HighwayInfoKafkaMessage;
import e63.course.kafka_decoders.DateDecoder;
import e63.course.kafka_decoders.HighwayInfoKafkaDecoder;
import e63.course.streaming.utils.FileWriterForLocalAndS3;
import e63.course.streaming.utils.HighwayInfoConstants;
import scala.Tuple2;

/**
 * This class is for spark streaming consumer of final project of e63 course
 * (Big Data Analytics) of Harvard Extension School
 * 
 * This class needs 3 parameters for execution. The way to execute this class
 * is: HighwayInfoKafkaSlidingWIndowConsumer <kafka_brokers> <kafka_topics>
 * <zookeeper>
 * 
 * @author Rohan Pulekar
 *
 */
public class HighwayInfoKafkaSlidingWindowConsumer {

	// initialize the logger that is based on slf4j library
	private static final Logger LOGGER = LoggerFactory.getLogger(HighwayInfoKafkaSlidingWindowConsumer.class);

	// date format for printing times in log output
	private static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("hh:mm:ss");

	/**
	 * The main function for this class
	 * 
	 * @param args
	 *            (<kafka_brokers> <kafka_topics> <zookeeper>)
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		// make sure the 3 arguments are passed to the program
		if (args.length < 3) {
			System.err
					.println("Usage: HighwayInfoKafkaSlidingWIndowConsumer <kafkaBrokers> <kafka_topics> <zookeeper>\n"
							+ " <kafka_brokers> is a list of one or more Kafka brokers e.g. ec2-54-173-167-157.compute-1.amazonaws.com:9092\n"
							+ " <kafka_topics> is a list of one or more kafka topics to consume from e.g. highway_info_kafka_topic\n"
							+ " <zookeeper> is the IP and port on which server is running e.g. ec2-54-173-167-157.compute-1.amazonaws.com:2181\n");
			System.exit(1);
		}

		// for this project, the kafka broker is on ec2: this is:
		// ec2-54-173-167-157.compute-1.amazonaws.com:9092
		String kafkaBrokers = args[0];

		// for this project, the kafka topic is: highway_info_kafka_topic
		String kafkaTopic = args[1];

		// for this project, this is:
		// ec2-54-173-167-157.compute-1.amazonaws.com:2181
		String zookeeper = args[2];

		// Create a Java Spark Config
		SparkConf sparkConf = new SparkConf().setMaster("local")
				.setAppName(HighwayInfoKafkaSlidingWindowConsumer.class.getSimpleName());

		// create spark context from the spark configuration
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// create sqlcontext
		final SQLContext sqlContext = new SQLContext(sparkContext);

		// create spark streaming context with batch interval of given number of
		// seconds
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext,
				Durations.seconds(HighwayInfoConstants.STREAMING_BATCH_DURATION_IN_SECS));

		// create a kafka topics set
		HashSet<String> kafkaTopicsSet = new HashSet<String>(Arrays.asList(kafkaTopic.split(",")));

		// create a map for kafka input DStream params
		HashMap<String, String> kafkaInputDStreamParams = new HashMap<String, String>();
		kafkaInputDStreamParams.put("metadata.broker.list", kafkaBrokers);
		kafkaInputDStreamParams.put("zookeeper.connect", zookeeper);
		kafkaInputDStreamParams.put("group.id", HighwayInfoKafkaSlidingWindowConsumer.class.getSimpleName());

		// print messages
		System.out.println("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);
		LOGGER.info("Listening for kafka messages from " + kafkaBrokers + "  on topic:" + kafkaTopic);

		// create a pair input DStream
		JavaPairInputDStream<Date, HighwayInfoKafkaMessage> highwayAndSpeedsInputDStream = KafkaUtils
				.createDirectStream(streamingContext, Date.class, HighwayInfoKafkaMessage.class, DateDecoder.class,
						HighwayInfoKafkaDecoder.class, kafkaInputDStreamParams, kafkaTopicsSet);

		// create a sliding window stream
		// the window duration is WINDOW_DURATION_IN_SECS seconds
		// and slide interval is SLIDE_INTERVAL_IN_SECS seconds
		JavaPairDStream<Date, HighwayInfoKafkaMessage> highwayAndSpeedsDStream = highwayAndSpeedsInputDStream.window(
				Durations.seconds(HighwayInfoConstants.WINDOW_DURATION_IN_SECS),
				Durations.seconds(HighwayInfoConstants.SLIDE_INTERVAL_IN_SECS));

		// this function is to access the continuous DStream
		VoidFunction<JavaPairRDD<Date, HighwayInfoKafkaMessage>> functionToProcessEachRDD = new VoidFunction<JavaPairRDD<Date, HighwayInfoKafkaMessage>>() {

			@Override
			protected Object clone() throws CloneNotSupportedException {
				return super.clone();
			}

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<Date, HighwayInfoKafkaMessage> highwayAndSpeedRDD) throws Exception {

				// sort the RDD elements
				JavaPairRDD<Date, HighwayInfoKafkaMessage> sortedHighwayAndSpeedRDD = highwayAndSpeedRDD.sortByKey();

				// create a new local csv file and log a message
				FileWriterForLocalAndS3.createNewLocalCSVFile();
				System.out.println(DATE_TIME_FORMAT.format(new Date()) + "	 created new local csv and S3 file");
				LOGGER.info("created new local csv file");

				// call functionToProcessEachTupleOfRDD for each tuple
				sortedHighwayAndSpeedRDD.foreach(functionToProcessEachTupleOfRDD);

				// call the below function to flush out buffer to local and S3
				// CSV file and the log a corresponding message
				FileWriterForLocalAndS3.flushBufferToLocalAndS3Files(sqlContext);
				System.out.println(
						DATE_TIME_FORMAT.format(new Date()) + "	 wrote contents to the local csv and S3 file");
				LOGGER.info("wrote contents to the local csv and S3 file");
			}

			// this function is to read each tuple of RDD and then write that
			// tuple to the local and S3 CSV files
			VoidFunction<Tuple2<Date, HighwayInfoKafkaMessage>> functionToProcessEachTupleOfRDD = new VoidFunction<Tuple2<Date, HighwayInfoKafkaMessage>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<Date, HighwayInfoKafkaMessage> tuple) throws Exception {

					System.out.println("tuple:" + tuple._1 + ":" + tuple._2);

					// add the RDD tuple to file writer buffer
					FileWriterForLocalAndS3.addTupleToFileWriteBuffer(tuple);
				}
			};
		};

		// call functionToLogOutEachRDD on each RDD of streaming DStream so
		// that the RDDs can be processed
		highwayAndSpeedsDStream.foreachRDD(functionToProcessEachRDD);

		// Start the computation and await termination
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}