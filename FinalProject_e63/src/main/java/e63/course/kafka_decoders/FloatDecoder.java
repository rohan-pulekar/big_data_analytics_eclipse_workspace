package e63.course.kafka_decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

import kafka.serializer.Decoder;

public class FloatDecoder implements Decoder<Float> {

	public FloatDecoder(kafka.utils.VerifiableProperties properties) {

	}

	@Override
	public Float fromBytes(byte[] bytes) {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			Object o = in.readObject();
			Float desirializedFloat = (Float) o;
			return desirializedFloat;
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
