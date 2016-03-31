package e63.kafka.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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

public class Problem3_Consumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(Problem3_Consumer.class);

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: Problem3_Consumer <brokers> <topics>\n"
					+ " <brokers> is a list of one or more Kafka brokers\n"
					+ " <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0]; // "localhost:9092";
		String kafkaTopic = args[1]; // "assignment8_kafka_topic";

		JavaSparkContext sparkContext = new JavaSparkContext("local[5]", "Problem3_Consumer");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(kafkaTopic.split(",")));

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("zookeeper.connect", "localhost:2181");
		kafkaParams.put("group.id", "Problem3_Consumer");

		LOGGER.info("Listening for kafka messages from " + brokers + "  on topic:" + kafkaTopic);

		JavaPairInputDStream<String, String> pairInputDStream = KafkaUtils.createDirectStream(streamingContext,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		JavaDStream<String> numbersReceivedAsMessages = pairInputDStream
				.map(new Function<Tuple2<String, String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		JavaPairDStream<String, Integer> numberAndCountAsOnePairs = numbersReceivedAsMessages
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		Function2<Integer, Integer, Integer> reduceFunction = new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1 + i2;
			}
		};

		JavaPairDStream<String, Integer> numberAndCountPairs = numberAndCountAsOnePairs
				.reduceByKeyAndWindow(reduceFunction, Durations.seconds(30), Durations.seconds(5));

		VoidFunction<JavaPairRDD<String, Integer>> functionToAccessEachRDD = new VoidFunction<JavaPairRDD<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> javaPairRDD) throws Exception {
				javaPairRDD.foreach(functionToPrintLog);
			}

			VoidFunction<Tuple2<String, Integer>> functionToPrintLog = new VoidFunction<Tuple2<String, Integer>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<String, Integer> tuple) throws Exception {
					LOGGER.info("Number:" + tuple._1() + " count:" + tuple._2());
				}
			};
		};

		numberAndCountPairs.foreachRDD(functionToAccessEachRDD);

		// Start the computation and await termination
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
