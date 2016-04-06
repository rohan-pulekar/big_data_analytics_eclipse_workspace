package e63.kafka.streaming;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * This class is for Assignment8 Problem2 of e63 course (Big Data Analytics) at
 * Harvard Extension School. The class can be run as a JAVA program in the
 * following way: Problem2_Consumer_KafkaAPI <serverIPRunningKafka>
 * <kafkaPortNum> <kafkaTopics> <partitionNum>
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem2_Consumer_KafkaAPI {

	// use File Writer to output the messages
	private static FileWriter fileWriter = null;

	// create a list that will hold replica brokers
	private List<String> m_replicaBrokers = new ArrayList<String>();

	private static final int KAFKA_CONSUMER_TIMEOUT = 100000;

	private static final int KAFKA_CONSUMER_BUFFER_SIZE = 64 * 1024;

	// date format for printing times in log output
	private static final DateFormat dateTimeFormat = new SimpleDateFormat("hh:mm:ss");

	/**
	 * the main function that expects 4 input parameters
	 * 
	 * @param args
	 *            (<serverIPRunningKafka> <kafkaPortNum> <kafkaTopics>
	 *            <partitionNum>)
	 */
	public static void main(String args[]) {

		// this is a validation to make sure that all 4 parameters are
		// passed to the program
		if (args.length < 4) {
			System.err.println(
					"Usage: Problem2_Consumer_KafkaAPI <serverIPRunningKafka> <kafkaPortNum> <kafkaTopics> <partitionNum>\n"
							+ " <serverIPRunningKafka> is a list of one or more Kafka broker IPs e.g. localhost\n"
							+ " <kafkaPortNum> is the port number on which is running e.g. 9092\n"
							+ " <kafkaTopics> is a list of one or more kafka topics to consume from e.g. assignment8_problem2and3_topic\n"
							+ " <partitionNum> is the parititon number e.g. 0\n");

			// exit the program if all params are not supplied
			System.exit(1);
		}

		try {
			// initialize the file writer
			fileWriter = new FileWriter(new File("./problem2_consumer_KafkaAPI.log"));
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		// for my local instance this will be "localhost";
		String serverIPRunningKafka = args[0];

		// for my local instance this is 9092
		int portNumberRunningKafka = Integer.parseInt(args[1]);

		// for my local instance this is "assignment8_problem2and3_topic"
		String kafkaTopic = args[2];

		// for my local instance this can be 0 or 1 or 2 or 3
		int kafkaPartition = Integer.parseInt(args[3]);

		// create an instance of the class
		Problem2_Consumer_KafkaAPI example = new Problem2_Consumer_KafkaAPI();

		// create a list of kafka broker IP addresses
		List<String> kafkaBrokerIPs = new ArrayList<String>();
		kafkaBrokerIPs.add(serverIPRunningKafka);

		// log out a message saying consumer listening
		System.out.println("Listening for kafka messages from " + serverIPRunningKafka + ":" + portNumberRunningKafka
				+ "  on topic:" + kafkaTopic);
		try {
			fileWriter.write("Listening for kafka messages from " + serverIPRunningKafka + ":" + portNumberRunningKafka
					+ "  on topic:" + kafkaTopic);
			fileWriter.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0);
		}

		try {
			// invoke the function to listen to messages
			example.listenToKafkaMessages(kafkaTopic, kafkaPartition, kafkaBrokerIPs, portNumberRunningKafka);

		} catch (Exception e) {

			// log out the exception
			System.out.println("Exception encountered:" + e);
			e.printStackTrace();
		}
	}

	/*
	 * Default constructor. Initializes the list m_replicaBrokers
	 */
	public Problem2_Consumer_KafkaAPI() {
		m_replicaBrokers = new ArrayList<String>();
	}

	/**
	 * @param kafkaTopic
	 * @param kafkaPartition
	 * @param kafkaBrokerIPs
	 * @param portNumberRunningKafka
	 * @throws Exception
	 */
	public void listenToKafkaMessages(String kafkaTopic, int kafkaPartition, List<String> kafkaBrokerIPs,
			int portNumberRunningKafka) throws Exception {

		// find metadata about the topic and partition we are interested in
		PartitionMetadata metadata = findLeader(kafkaBrokerIPs, portNumberRunningKafka, kafkaTopic, kafkaPartition);
		if (metadata == null) {
			// metadata not found for the partition. Which means partition
			// indicated by kafkaPartition variable does nto exist
			System.out.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}

		if (metadata.leader() == null) {
			// this means we are not able to find kafka broker leader.
			System.out.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}

		// get the host IP of the kafka broker leader
		String leadKafkaBrokerIP = metadata.leader().host();

		// create a name for kafka client
		String kafkaConsumerClientName = "Client_" + kafkaTopic + "_" + kafkaPartition;

		// create an instance of SimpleConsumer
		SimpleConsumer kafkaSimpleConsumer = new SimpleConsumer(leadKafkaBrokerIP, portNumberRunningKafka,
				KAFKA_CONSUMER_TIMEOUT, KAFKA_CONSUMER_BUFFER_SIZE, kafkaConsumerClientName);

		// get last offset of messages based on the lastest time policy. So,
		// only the messages sent to kafka after this Consumer starts will be
		// read by this consumer. Messages sent before this Consumer starts will
		// not be read
		long readOffsetForReadingMessages = getLastOffset(kafkaSimpleConsumer, kafkaTopic, kafkaPartition,
				kafka.api.OffsetRequest.LatestTime(), kafkaConsumerClientName);

		// keep a track of number of errors
		int numErrors = 0;

		// this is indefinite loop. So, messages will be read till this Consumer
		// is terminated
		while (true) {

			// if kafkaSimpleConsumer is closed or becomes null in this loop
			// then initialize it again
			if (kafkaSimpleConsumer == null) {
				kafkaSimpleConsumer = new SimpleConsumer(leadKafkaBrokerIP, portNumberRunningKafka,
						KAFKA_CONSUMER_TIMEOUT, KAFKA_CONSUMER_BUFFER_SIZE, kafkaConsumerClientName);
			}

			// create kafka message fetch request
			FetchRequest messageFetchRequest = new FetchRequestBuilder().clientId(kafkaConsumerClientName)
					.addFetch(kafkaTopic, kafkaPartition, readOffsetForReadingMessages, KAFKA_CONSUMER_TIMEOUT).build();

			// get the kafka message fetch response
			FetchResponse messageFetchResponse = kafkaSimpleConsumer.fetch(messageFetchRequest);

			if (messageFetchResponse.hasError()) {
				// this means message fetch response has an error

				// increment the error counter
				numErrors++;

				// get the error code
				short code = messageFetchResponse.errorCode(kafkaTopic, kafkaPartition);

				// log out the error message
				fileWriter.write("\nError fetching data from the Broker:" + leadKafkaBrokerIP + " Reason: " + code);

				if (numErrors > 5) {
					// this means there are too many errors. break out of the
					// loop
					break;
				}

				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// This means we asked for an invalid offset. For simple
					// case ask for the last element to reset and continue the
					// loop
					readOffsetForReadingMessages = getLastOffset(kafkaSimpleConsumer, kafkaTopic, kafkaPartition,
							kafka.api.OffsetRequest.LatestTime(), kafkaConsumerClientName);
					continue;
				}

				// close out the kafkaSimpelConsumer
				kafkaSimpleConsumer.close();
				kafkaSimpleConsumer = null;

				// and find a new kafka broker leader
				leadKafkaBrokerIP = findNewKafkaBrokerLeader(leadKafkaBrokerIP, kafkaTopic, kafkaPartition,
						portNumberRunningKafka);
				continue;
			}

			// reset the error counter
			numErrors = 0;

			long numberOfMessagesRead = 0;

			// loop for each message in the message fetch response
			for (MessageAndOffset messageAndOffset : messageFetchResponse.messageSet(kafkaTopic, kafkaPartition)) {

				// get the message offset
				long currentMessageOffset = messageAndOffset.offset();

				// check if the offset is correct
				if (currentMessageOffset < readOffsetForReadingMessages) {
					fileWriter.write("\nFound an old offset: " + currentMessageOffset + " Expecting: "
							+ readOffsetForReadingMessages);
					continue;
				}

				// get the next message offset
				readOffsetForReadingMessages = messageAndOffset.nextOffset();

				// get the message payload
				ByteBuffer payload = messageAndOffset.message().payload();

				// create a bytes array of the size of payload
				byte[] bytes = new byte[payload.limit()];

				// get the payload bytes
				payload.get(bytes);

				System.out.println("Time: " + dateTimeFormat.format(new Date())
						+ "   Kafka Consumer: Received message in the offset:"
						+ String.valueOf(messageAndOffset.offset()) + " The message is: " + new String(bytes, "UTF-8"));
				fileWriter.write("\nTime: " + dateTimeFormat.format(new Date())
						+ "   Kafka Consumer: Received message in the offset:"
						+ String.valueOf(messageAndOffset.offset()) + " The message is: " + new String(bytes, "UTF-8"));
				fileWriter.flush();
				// increment the number of messages counter
				numberOfMessagesRead++;
			}

			if (numberOfMessagesRead == 0) {
				// this means no new messages were read. Sleep for 1 sec in that
				// case
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}

		// close the kafka simple consumer
		if (kafkaSimpleConsumer != null) {
			kafkaSimpleConsumer.close();
		}
	}

	/**
	 * This function gets the last offset for the given kafka simpel consumer,
	 * topic, partition, time and client
	 * 
	 * @param kafkaSimpleConsumer
	 * @param kafkaTopic
	 * @param kafkaPartition
	 * @param whichTime
	 * @param clientName
	 * @return offset
	 */
	public static long getLastOffset(SimpleConsumer kafkaSimpleConsumer, String kafkaTopic, int kafkaPartition,
			long whichTime, String clientName) {

		// create a topic and partition
		TopicAndPartition topicAndPartition = new TopicAndPartition(kafkaTopic, kafkaPartition);

		// create and populate a kafka request info
		Map<TopicAndPartition, PartitionOffsetRequestInfo> kafkaRequestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		kafkaRequestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

		// create kafka offset request
		kafka.javaapi.OffsetRequest kafkaOffsetRequest = new kafka.javaapi.OffsetRequest(kafkaRequestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);

		// get kafka offset response
		OffsetResponse kafkaOffsetResponse = kafkaSimpleConsumer.getOffsetsBefore(kafkaOffsetRequest);

		if (kafkaOffsetResponse.hasError()) {
			// kafka offset response has error
			System.out.println("Error fetching data Offset Data the Broker. Reason: "
					+ kafkaOffsetResponse.errorCode(kafkaTopic, kafkaPartition));
			return 0;
		}

		// get the offsets from kafka offset response
		long[] offsets = kafkaOffsetResponse.offsets(kafkaTopic, kafkaPartition);
		return offsets[0];
	}

	/**
	 * This function finds a new kafka broker leader
	 * 
	 * @param kafkaOldBrokerLeader
	 * @param kafkaTopic
	 * @param kafkaPartition
	 * @param kafkServerPort
	 * @return IP address of new kafka broker leader
	 * @throws Exception
	 */
	private String findNewKafkaBrokerLeader(String kafkaOldBrokerLeader, String kafkaTopic, int kafkaPartition,
			int kafkServerPort) throws Exception {

		// loop thrice to find the new broker leader
		for (int counter = 0; counter < 3; counter++) {

			// variable to keep track of whether the consumer should sleep
			// momentarily
			boolean goToSleep = false;

			// invoke find leader to find broker leader
			PartitionMetadata partitionMetadata = findLeader(m_replicaBrokers, kafkServerPort, kafkaTopic,
					kafkaPartition);

			if (partitionMetadata == null) {
				// this means no metadata received for new broker leader
				goToSleep = true;
			} else if (partitionMetadata.leader() == null) {
				// this means no metadata received for new broker leader
				goToSleep = true;
			} else if (kafkaOldBrokerLeader.equalsIgnoreCase(partitionMetadata.leader().host()) && counter == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				goToSleep = true;
			} else {
				// we got a new broker leader. return its IP
				return partitionMetadata.leader().host();
			}
			if (goToSleep) {
				try {
					// let the consumer sleep for 1 sec
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		fileWriter.write("\nUnable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	/**
	 * 
	 * This function finds a kafka broker leader
	 * 
	 * @param kafkaBrokers
	 * @param kafkaServerPort
	 * @param kafkaTopic
	 * @param kafkaPartitionNumber
	 * @return kafka partition metadata of new leader
	 */
	private PartitionMetadata findLeader(List<String> kafkaBrokers, int kafkaServerPort, String kafkaTopic,
			int kafkaPartitionNumber) {

		PartitionMetadata partitionMetaDataToReturn = null;

		// loop through all the brokers
		loop: for (String kafkaBroker : kafkaBrokers) {

			SimpleConsumer kafkaSimpleConsumer = null;
			try {
				// initialize a kafka simple consumer
				kafkaSimpleConsumer = new SimpleConsumer(kafkaBroker, kafkaServerPort, KAFKA_CONSUMER_TIMEOUT,
						KAFKA_CONSUMER_BUFFER_SIZE, "leaderLookup");

				// create a list of kafka topics
				List<String> kafkaTopics = Collections.singletonList(kafkaTopic);

				// create a kafka topic metadata request
				TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(kafkaTopics);

				// get the topic metadata response
				kafka.javaapi.TopicMetadataResponse topicMetadataResponse = kafkaSimpleConsumer
						.send(topicMetadataRequest);

				// get the list of topics metadata
				List<TopicMetadata> topicMetadataList = topicMetadataResponse.topicsMetadata();

				// loop through the topic metadata list
				for (TopicMetadata topicMetadata : topicMetadataList) {

					// loop through the partirion metadata
					for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {

						if (partitionMetadata.partitionId() == kafkaPartitionNumber) {
							// this means partitionMetadata belongs to the
							// partition we are looking for
							partitionMetaDataToReturn = partitionMetadata;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + kafkaBroker + "] to find Leader for ["
						+ kafkaTopic + ", " + kafkaPartitionNumber + "] Reason: " + e);
			} finally {
				if (kafkaSimpleConsumer != null)
					kafkaSimpleConsumer.close();
			}
		}
		if (partitionMetaDataToReturn != null) {

			// set the replicaBrokers to replica brokers pointed by partition
			// meta data we found
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replicaBroker : partitionMetaDataToReturn.replicas()) {
				m_replicaBrokers.add(replicaBroker.host());
			}
		}

		// return the partition metadata
		return partitionMetaDataToReturn;
	}
}