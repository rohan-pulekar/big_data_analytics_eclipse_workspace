package e63.course.streaming.producer;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import e63.course.dtos.HighwayInfoKafkaMessage;
import e63.course.dtos.MassachusettsHighway;
import e63.course.streaming.utils.HighwayInfoConstants;
import e63.course.streaming.utils.LiveXmlFeedReader;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * This class is for spark streaming producer of final project of e63 course
 * (Big Data Analytics) of Harvard Extension School
 * 
 * This class needs 3 parameters for execution. The way to execute this class
 * is: HighwayInfoKafkaProducer <kafkaServerIPAndPort> <kafkaTopic>
 * 
 * @author Rohan Pulekar
 *
 */
public class HighwayInfoKafkaProducer {

	// initialize the logger which is based on slf4j library
	private static final Logger LOGGER = LoggerFactory.getLogger(HighwayInfoKafkaProducer.class);

	private static final int NUMBER_OF_MILLI_SECS_IN_SLIDE_INTERVAL = 1000
			* HighwayInfoConstants.SLIDE_INTERVAL_IN_SECS;

	/**
	 * The main function for this class
	 * 
	 * @param args
	 *            (<kafkaServerIPAndPort> <kafkaTopic>)
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {

		// make sure 2 arguments are passed to the program
		if (args.length < 2) {
			System.err.println("Usage: HighwayInfoKafkaProducer <kafkaServerIPAndPort> <kafkaTopic>\n"
					+ " <kafkaServerIPAndPort> is a list of one or more Kafka brokers e.g. ec2-54-173-167-157.compute-1.amazonaws.com:9092\n"
					+ " <kafkaTopic> is a list of one or more kafka topics to consume from e.g. highway_info_kafka_topic\n\n");
			System.exit(1);
		}

		// for this project, kafka is on ec2 and so this parameter will be
		// ec2-54-173-167-157.compute-1.amazonaws.com:9092
		String kafkaServerIPAndPort = args[0];

		// for my local instance this is "highway_info_kafka_topic"
		String kafkaTopic = args[1];

		// create properties map for kafka producer
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put("metadata.broker.list", kafkaServerIPAndPort);
		kafkaProducerProperties.put("key.serializer.class", e63.course.kafka_encoders.DateEncoder.class.getName());
		kafkaProducerProperties.put("serializer.class",
				e63.course.kafka_encoders.HighwayInfoKafkaEncoder.class.getName());
		kafkaProducerProperties.put("request.required.acks", "1");

		// create kafka producer config
		ProducerConfig kafkaProducerConfig = new ProducerConfig(kafkaProducerProperties);

		// create kafka producer
		Producer<Date, HighwayInfoKafkaMessage> kafkaProducer = new Producer<Date, HighwayInfoKafkaMessage>(
				kafkaProducerConfig);

		// declare a variable for kafka keyed message
		KeyedMessage<Date, HighwayInfoKafkaMessage> kafkaKeyedMessage = null;

		// this is an infinite loop. so basically producer will keep messages
		// until the program is shutdown
		while (true) {

			// get time for EST (UTC -4)
			Calendar calendar = Calendar.getInstance();
			// calendar.add(Calendar.HOUR, -4);
			Date time = calendar.getTime();

			// process the live Mass DOT xml stream and highway and speed map
			// out of it
			Map<MassachusettsHighway, Double> highwayAndSpeedMap = LiveXmlFeedReader.processLiveXmlStream();

			if (highwayAndSpeedMap != null) {
				// run a loop for each massachusetts highway
				for (Entry<MassachusettsHighway, Double> entry : highwayAndSpeedMap.entrySet()) {

					// create a new highway info kafka message which will store
					// highway name and average speed on the highway
					HighwayInfoKafkaMessage highwayInfoKafkaMessage = new HighwayInfoKafkaMessage(entry.getKey(),
							entry.getValue());

					// create a kafka keyed message with key=current_time,
					// value=kafka message created above
					kafkaKeyedMessage = new KeyedMessage<Date, HighwayInfoKafkaMessage>(kafkaTopic, time,
							highwayInfoKafkaMessage);

					// send the kafka message to kafka broker
					kafkaProducer.send(kafkaKeyedMessage);
				}

				// log a message indicating message sent
				LOGGER.info("sent kafka messages");
			}

			// find out number of millisecs till the start of next streaming
			// slide interval
			long millisWithinMin = System.currentTimeMillis() % NUMBER_OF_MILLI_SECS_IN_SLIDE_INTERVAL;

			// sleep until the start of next minute
			Thread.sleep(NUMBER_OF_MILLI_SECS_IN_SLIDE_INTERVAL - millisWithinMin);
		}
	}
}
