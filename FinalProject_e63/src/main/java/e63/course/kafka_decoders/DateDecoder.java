package e63.course.kafka_decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Date;

import kafka.serializer.Decoder;

public class DateDecoder implements Decoder<Date> {

	public DateDecoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public Date fromBytes(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			Object o = in.readObject();
			Date desirializedDate = (Date) o;
			return desirializedDate;
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
