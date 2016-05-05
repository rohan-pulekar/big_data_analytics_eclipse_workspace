package e63.course.final_project;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

import e63.course.dtos.HighwayInfoKafkaMessage;
import e63.course.dtos.MassachusettsHighway;
import e63.course.kafka_decoders.DateDecoder;
import e63.course.kafka_decoders.HighwayInfoKafkaDecoder;
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
	private static final DateFormat DATE_TIME_FORMAT = new SimpleDateFormat("hh:mm:ss");

	private static int STREAMING_BATCH_DURATION_IN_SECS = 60;// 1 min

	private static int WINDOW_DURATION_IN_SECS = 120;// 2 mins

	private static int SLIDE_INTERVAL_IN_SECS = 60;// 1 min

	/**
	 * The main function for this class
	 * 
	 * @param args
	 *            (<brokers> <topics>)
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

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
		JavaPairInputDStream<Date, HighwayInfoKafkaMessage> highwayAndSpeedsInputDStream = KafkaUtils
				.createDirectStream(streamingContext, Date.class, HighwayInfoKafkaMessage.class, DateDecoder.class,
						HighwayInfoKafkaDecoder.class, kafkaInputDStreamParams, kafkaTopicsSet);

		// here reduceByKeyAndWindow is used to create a sliding windowed
		// stream.
		// the window duration is WINDOW_DURATION_IN_SECS seconds
		// and slide interval is SLIDE_INTERVAL_IN_SECS seconds
		JavaPairDStream<Date, HighwayInfoKafkaMessage> highwayAndSpeedsDStream = highwayAndSpeedsInputDStream
				.window(Durations.seconds(WINDOW_DURATION_IN_SECS), Durations.seconds(SLIDE_INTERVAL_IN_SECS));

		// this function is to access each RDD
		VoidFunction<JavaPairRDD<Date, HighwayInfoKafkaMessage>> functionToLogOutEachRDD = new VoidFunction<JavaPairRDD<Date, HighwayInfoKafkaMessage>>() {

			@Override
			protected Object clone() throws CloneNotSupportedException {
				// TODO Auto-generated method stub
				return super.clone();
			}

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<Date, HighwayInfoKafkaMessage> highwayAndSpeedRDD) throws Exception {

				createNewCSVFileWithHeaders();
				System.out.println(DATE_TIME_FORMAT.format(new Date()) + "	 created new file");

				// some calendar and date operations to print time stamps for
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.SECOND, -1 * WINDOW_DURATION_IN_SECS);
				Date currentTimeStamp = new Date();
				Date timeStampAtStartOfWindow = calendar.getTime();
				// System.out.println("");
				// System.out.println("Current time: " +
				// dateTimeFormat.format(currentTimeStamp)
				// + " showing highway and speed in last " +
				// WINDOW_DURATION_IN_SECS + " secs (from "
				// + dateTimeFormat.format(timeStampAtStartOfWindow) + " to
				// now)");

				JavaPairRDD<Date, HighwayInfoKafkaMessage> sortedHighwayAndSpeedRDD = highwayAndSpeedRDD.sortByKey();

				// for each function to log out each tuple of number and count
				sortedHighwayAndSpeedRDD.foreach(functionToLogOutEachTuple);
			}

			// this function is to access each tuple in the RDD and then print
			// that tuple to the log wih LOGGER
			VoidFunction<Tuple2<Date, HighwayInfoKafkaMessage>> functionToLogOutEachTuple = new VoidFunction<Tuple2<Date, HighwayInfoKafkaMessage>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<Date, HighwayInfoKafkaMessage> tuple) throws Exception {
					// System.out.println("Highway:" + tuple._1() + " Speed:" +
					// tuple._2());
					Date timeStamp = tuple._1();
					HighwayInfoKafkaMessage highwayInfoKafkaMessage = tuple._2();
					writeCsvFileContents(timeStamp, highwayInfoKafkaMessage.getHighway(),
							highwayInfoKafkaMessage.getSpeed());
				}
			};

			private void createNewCSVFileWithHeaders() throws IOException {
				File csvOutputFile = new File(HighwayInfoConstants.CSV_OUTPUT_FILE_NAME);
				if (csvOutputFile.exists() && csvOutputFile.isFile()) {
					csvOutputFile.delete();
				}
				csvOutputFile.createNewFile();
				FileWriter csvOutputFileWriter = new FileWriter(csvOutputFile);
				csvOutputFileWriter.append("Time");
				csvOutputFileWriter.append(',');
				csvOutputFileWriter.append("Highway");
				csvOutputFileWriter.append(',');
				csvOutputFileWriter.append("Speed");
				csvOutputFileWriter.append('\n');
				csvOutputFileWriter.flush();
				csvOutputFileWriter.close();
			}

			private void writeCsvFileContents(Date timeStamp, MassachusettsHighway highway, Float speed)
					throws IOException {
				File csvOutputFile = new File(HighwayInfoConstants.CSV_OUTPUT_FILE_NAME);
				FileWriter csvOutputFileWriter = new FileWriter(csvOutputFile, true);
				csvOutputFileWriter.write(HighwayInfoConstants.DATE_FORMATTER_FOR_TIME.format(timeStamp));
				csvOutputFileWriter.append(',');
				csvOutputFileWriter.write(String.valueOf(highway));
				csvOutputFileWriter.append(',');
				csvOutputFileWriter.write(HighwayInfoConstants.DECIMAL_FORMAT_WITH_ROUNDING.format(speed));
				csvOutputFileWriter.append('\n');
				csvOutputFileWriter.flush();
				csvOutputFileWriter.close();
			}
		};

		// call functionToLogOutEachRDD on each RDD of numberAndCountPairs so
		// that the RDDs can be printed out in logs
		highwayAndSpeedsDStream.foreachRDD(functionToLogOutEachRDD);

		// Start the computation and await termination
		streamingContext.start();
		streamingContext.awaitTermination();
	}

}
