package e63.course.kafka_decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import e63.course.dtos.HighwayInfoKafkaMessage;
import kafka.serializer.Decoder;

/**
 * This is a class for Kafka decoding of Highway Info class
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class HighwayInfoKafkaDecoder implements Decoder<HighwayInfoKafkaMessage> {

	public HighwayInfoKafkaDecoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public HighwayInfoKafkaMessage fromBytes(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			Object o = in.readObject();
			HighwayInfoKafkaMessage desirializedHighwayInfoKafkaMessage = (HighwayInfoKafkaMessage) o;
			return desirializedHighwayInfoKafkaMessage;
		} catch (ClassNotFoundException exp) {
			exp.printStackTrace();
		} catch (IOException exp) {
			exp.printStackTrace();
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return null;
	}
}
