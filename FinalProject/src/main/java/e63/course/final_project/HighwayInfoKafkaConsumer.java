package e63.course.final_project;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import e63.course.dtos.HighwayInfoKafkaMessage;
import e63.course.kafka_decoders.DateDecoder;
import e63.course.kafka_decoders.HighwayInfoKafkaDecoder;

/**
 * This class is for Assignment8 Problem2 of e63 course (Big Data Analytics) at
 * Harvard Extension School. The class can be run as a JAVA program in the
 * following way: Problem2_Consumer_KafkaAPI <serverIPRunningKafka>
 * <kafkaPortNum> <kafkaTopics> <numberOfPartitions>
 * 
 * This class uses the newer approach to receive data from Kafka
 * 
 * @author Rohan Pulekar
 *
 */
public class HighwayInfoKafkaConsumer {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: HighwayInfoKafkaConsumer <kafkaBrokers> <topics> <zookeeper>\n"
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
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("HighwayInfoKafkaConsumer");
		// sparkConf.setMaster("local[5]"); // this is to run the program as a
		// standalone java application

		// create spark context from the spark configuration
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// create spark streaming context with batch interval of 1 second
		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(2));

		// create a kafka topics set
		HashSet<String> kafkaTopicsSet = new HashSet<String>(Arrays.asList(kafkaTopic.split(",")));

		// create a map for kafka input DStream params
		HashMap<String, String> kafkaInputDStreamParams = new HashMap<String, String>();
		kafkaInputDStreamParams.put("metadata.broker.list", kafkaBrokers);
		kafkaInputDStreamParams.put("zookeeper.connect", zookeeper);
		kafkaInputDStreamParams.put("group.id", "HighwayInfoKafkaConsumer");

		System.out
				.println("Listening for kafka messages from kafka broker:" + kafkaBrokers + "  on topic:" + kafkaTopic);

		// create a pair input DStream
		JavaPairInputDStream<Date, HighwayInfoKafkaMessage> pairInputDStream = KafkaUtils.createDirectStream(
				streamingContext, Date.class, HighwayInfoKafkaMessage.class, DateDecoder.class,
				HighwayInfoKafkaDecoder.class, kafkaInputDStreamParams, kafkaTopicsSet);

		// create a stream of numbers received as messages
		pairInputDStream.print();

		// Start the computation
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}