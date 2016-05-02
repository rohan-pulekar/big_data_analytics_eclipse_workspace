package e63.course.final_project;

import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;

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
		kafkaProducerProperties.put("serializer.class", kafka.serializer.StringEncoder.class.getName());
		// properties.put("partitioner.class"
		kafkaProducerProperties.put("request.required.acks", "1");

		// create kafka producer config
		ProducerConfig kafkaProducerConfig = new ProducerConfig(kafkaProducerProperties);

		// create kafka producer
		Producer<String, String> kafkaProducer = new Producer<String, String>(kafkaProducerConfig);

		// declare a variable for kafka keyed message
		KeyedMessage<String, String> kafkaKeyedMessage = null;

		// this is an infinite loop. so basically producer will keep messages
		// until the program is shutdown
		while (true) {

			String xmlString = kafkaMessageProducer.readHighwayXmlStream();

			// create an instance of keyed message
			kafkaKeyedMessage = new KeyedMessage<String, String>(kafkaTopic,
					"hi" + DateFormatUtils.format(new Date(), "ss"));

			// send the message
			kafkaProducer.send(kafkaKeyedMessage);

			try {
				// sleep for 5 secs
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// this means the thread is interrupted by some other process.
				// Exit in that case
				System.exit(0);
			}
		}
	}

	private String readHighwayXmlStream() throws Exception {
		URL url = new URL("https://www.massdot.state.ma.us/feeds/traveltimes/RTTM_feed.aspx");
		URLConnection connection = url.openConnection();
		try {
			String xmlString = IOUtils.toString(connection.getInputStream(), "UTF-8");
			return xmlString;
		} catch (Exception ex) {
			throw ex;
		}
	}
}
