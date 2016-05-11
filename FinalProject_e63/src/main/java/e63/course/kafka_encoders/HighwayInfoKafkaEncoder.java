package e63.course.kafka_encoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import e63.course.dtos.HighwayInfoKafkaMessage;
import kafka.serializer.Encoder;

/**
 * This is a class for Kafka encoding of Highway Info object
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class HighwayInfoKafkaEncoder implements Encoder<HighwayInfoKafkaMessage> {

	public HighwayInfoKafkaEncoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public byte[] toBytes(HighwayInfoKafkaMessage highwayInfoKafkaMessage) {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(highwayInfoKafkaMessage);
			byte[] bytes = bos.toByteArray();
			return bytes;
		} catch (IOException exp) {
			exp.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return null;
	}
}
