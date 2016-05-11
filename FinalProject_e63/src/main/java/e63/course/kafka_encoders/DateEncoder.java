package e63.course.kafka_encoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Date;

import kafka.serializer.Encoder;

/**
 * This class is used for Kafka encoding of Date object
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class DateEncoder implements Encoder<Date> {

	public DateEncoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public byte[] toBytes(Date date) {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(date);
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
