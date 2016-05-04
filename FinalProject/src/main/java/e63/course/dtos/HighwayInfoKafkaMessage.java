package e63.course.dtos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class HighwayInfoKafkaMessage implements Serializable {
	private static final long serialVersionUID = 1L;

	public HighwayInfoKafkaMessage(Map<MassachusettsHighway, Float> highwaySpeedmap) {
		super();
		this.highwaySpeedMap = highwaySpeedmap;
	}

	private Map<MassachusettsHighway, Float> highwaySpeedMap = new HashMap<MassachusettsHighway, Float>();

	public Map<MassachusettsHighway, Float> getHighwaySpeedMap() {
		return highwaySpeedMap;
	}

	public void setHighwaySpeedMap(Map<MassachusettsHighway, Float> highwaySpeedmap) {
		this.highwaySpeedMap = highwaySpeedmap;
	}

	@Override
	public String toString() {
		if (highwaySpeedMap != null) {
			return highwaySpeedMap.toString();
		}
		return "";
	}
}
