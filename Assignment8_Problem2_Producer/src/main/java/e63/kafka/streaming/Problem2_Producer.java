package e63.kafka.streaming;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This class is for Assignment8 Problem2 of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * This class needs 2 parameters to run. This is how to run this class:
 * Problem2_Producer <kafkaServerIPAndPort> <kafkaTopic>
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem2_Producer {

	// use File Writer to output the messages
	private static FileWriter fileWriter = null;

	// date format for printing times in log output
	private static final DateFormat dateTimeFormat = new SimpleDateFormat("hh:mm:ss");

	/**
	 * the main function of this program
	 * 
	 * @param args
	 *            (<kafkaServerIPAndPort> <kafkaTopic>)
	 */
	public static void main(String[] args) {

		// make sure 2 arguments are passed to the program
		if (args.length < 2) {
			System.err.println("Usage: Problem2_Producer <kafkaServerIPAndPort> <kafkaTopic>\n"
					+ " <kafkaServerIPAndPort> is a list of one or more Kafka brokers e.g. localhost:9092\n"
					+ " <kafkaTopic> is a list of one or more kafka topics to consume from e.g. assignment8_problem2and3_topic\n\n");
			System.exit(1);
		}

		try {
			// initialize the file writer
			fileWriter = new FileWriter(new File("./problem2_producer.log"));
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		// for my local instance this is "localhost:9092"
		String kafkaServerIPAndPort = args[0];

		// for my local instance this is "assignment8_problem2and3_topic"
		String kafkaTopic = args[1];

		// create properties map for kafka producer
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("metadata.broker.list", kafkaServerIPAndPort);
		kafkaProducerProperties.put("serializer.class", kafka.serializer.StringEncoder.class.getName());
		// properties.put("partitioner.class"
		kafkaProducerProperties.put("request.required.acks", "1");

		// create kafka producer config
		ProducerConfig kafkaProducerConfig = new ProducerConfig(kafkaProducerProperties);

		// create kafka producer
		Producer<String, String> kafkaProducer = new Producer<String, String>(kafkaProducerConfig);

		// declare a variable for kafka keyed message
		KeyedMessage<String, String> kafkaKeyedMessage = null;

		// create a randm value generator
		Random randomValueGenerator = new Random();

		// log out a message saying sending kafka messages
		System.out.println("Will start sending kafka messages to " + kafkaServerIPAndPort + "  on topic:" + kafkaTopic);
		try {
			fileWriter
					.write("Will start sending kafka messages to " + kafkaServerIPAndPort + "  on topic:" + kafkaTopic);
			fileWriter.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// this is an infinite loop. so basically producer will keep messages
		// until the program is shutdown
		while (true) {

			// get a random number between 0 and 10
			int randomNumber = randomValueGenerator.nextInt(10);

			// create an instance of keyed message
			kafkaKeyedMessage = new KeyedMessage<String, String>(kafkaTopic, String.valueOf(randomNumber));

			// send the message
			kafkaProducer.send(kafkaKeyedMessage);

			// log out the message
			System.out.println(
					"Time: " + dateTimeFormat.format(new Date()) + "   Kafka Producer: Sent message :" + randomNumber);
			try {
				fileWriter.write("\nTime: " + dateTimeFormat.format(new Date()) + "   Kafka Producer: Sent message :"
						+ randomNumber);
				fileWriter.flush();
			} catch (IOException e1) {
				e1.printStackTrace();
				System.exit(0);
			}

			try {
				// sleep for 1 sec
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// this means the thread is interrupted by some other process.
				// Exit in that case
				System.exit(0);
			}
		}
	}
}
