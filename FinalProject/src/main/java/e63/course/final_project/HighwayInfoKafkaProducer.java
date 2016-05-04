package e63.course.final_project;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import e63.course.dtos.MassachusettsHighway;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class HighwayInfoKafkaProducer {
	public static void main(String[] args) throws Exception {
		HighwayInfoKafkaProducer kafkaMessageProducer = new HighwayInfoKafkaProducer();

		// make sure 2 arguments are passed to the program
		if (args.length < 2) {
			System.err.println("Usage: Problem2_Producer <kafkaServerIPAndPort> <kafkaTopic>\n"
					+ " <kafkaServerIPAndPort> is a list of one or more Kafka brokers e.g. localhost:9092\n"
					+ " <kafkaTopic> is a list of one or more kafka topics to consume from e.g. assignment8_problem2and3_topic\n\n");
			System.exit(1);
		}

		// for my local instance this is "localhost:9092"
		String kafkaServerIPAndPort = args[0];

		// for my local instance this is "assignment8_problem2and3_topic"
		String kafkaTopic = args[1];

		// create properties map for kafka producer
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("metadata.broker.list", kafkaServerIPAndPort);
		kafkaProducerProperties.put("key.serializer.class", e63.course.kafka_encoders.HighwayEncoder.class.getName());
		kafkaProducerProperties.put("serializer.class", e63.course.kafka_encoders.FloatEncoder.class.getName());
		kafkaProducerProperties.put("request.required.acks", "1");

		// create kafka producer config
		ProducerConfig kafkaProducerConfig = new ProducerConfig(kafkaProducerProperties);

		// create kafka producer
		Producer<MassachusettsHighway, Float> kafkaProducer = new Producer<MassachusettsHighway, Float>(
				kafkaProducerConfig);

		// declare a variable for kafka keyed message
		KeyedMessage<MassachusettsHighway, Float> kafkaKeyedMessage = null;

		// this is an infinite loop. so basically producer will keep messages
		// until the program is shutdown
		while (true) {

			// Date time = new Date();

			Map<MassachusettsHighway, Float> highwayAndSpeedMap = XmlFileProcessor.processLiveXmlStream();

			if (highwayAndSpeedMap != null) {
				for (Entry<MassachusettsHighway, Float> entry : highwayAndSpeedMap.entrySet()) {
					// create an instance of keyed message
					kafkaKeyedMessage = new KeyedMessage<MassachusettsHighway, Float>(kafkaTopic, entry.getKey(),
							entry.getValue());

					// send the message
					kafkaProducer.send(kafkaKeyedMessage);
				}
			}

			// HighwayInfoKafkaMessage highwayInfoKafkaMessage = new
			// HighwayInfoKafkaMessage(highwayAndSpeedMap);

			// System.out.println("message sent");
		}
	}
}
