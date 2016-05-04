package e63.course.kafka_encoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import kafka.serializer.Encoder;

public class FloatEncoder implements Encoder<Float> {

	public FloatEncoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public byte[] toBytes(Float floatObject) {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(floatObject);
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
