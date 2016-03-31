package e63.kafka.streaming;

import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Problem2_Producer {

	private static final Logger LOGGER = LoggerFactory.getLogger(Problem2_Producer.class);

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: Problem2_Producer <kafkaServerIPAndPort> <kafkaTopic>\n"
					+ " <kafkaServerIPAndPort> is a list of one or more Kafka brokers\n"
					+ " <kafkaTopic> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String kafkaServerIPAndPort = args[0]; // "localhost:9092";
		String kafkaTopic = args[1]; // "assignment8_kafka_topic";

		Properties properties = new Properties();
		properties.put("metadata.broker.list", kafkaServerIPAndPort);
		properties.put("serializer.class", kafka.serializer.StringEncoder.class.getName());
		// properties.put("partitioner.class"
		properties.put("request.required.acks", "1");

		ProducerConfig producerConfig = new ProducerConfig(properties);

		Producer<String, String> kafkaProducer = new Producer<String, String>(producerConfig);

		KeyedMessage<String, String> keyedMessage = null;

		Random randomValueGenerator = new Random();

		LOGGER.info("Will start sending kafka messages to " + kafkaServerIPAndPort + "  on topic:" + kafkaTopic);

		while (true) {
			int randomNumber = randomValueGenerator.nextInt(10);

			keyedMessage = new KeyedMessage<String, String>(kafkaTopic, String.valueOf(randomNumber));

			kafkaProducer.send(keyedMessage);

			LOGGER.info("Kafka Producer: Sent message :" + randomNumber);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.exit(0);
			}
		}
	}
}
