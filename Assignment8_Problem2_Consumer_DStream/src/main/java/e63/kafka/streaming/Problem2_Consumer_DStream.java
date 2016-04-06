package e63.kafka.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * This class is for Assignment8 Problem2 of e63 course (Big Data Analytics) at
 * Harvard Extension School. The class can be run as a JAVA program in the
 * following way: Problem2_Consumer_DStream <brokers> <topics> <zookeeper>
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem2_Consumer_DStream {

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: Problem2_Consumer_DStream <kafkaBrokers> <topics> <zookeeper>\n"
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

		// print the messages to output
		numbersReceivedAsMessages.print();

		// Start the computation
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
