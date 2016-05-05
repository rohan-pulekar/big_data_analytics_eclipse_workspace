package e63.course.dtos;

import java.io.Serializable;

public class HighwayInfoKafkaMessage implements Serializable {
	private static final long serialVersionUID = 1L;

	public HighwayInfoKafkaMessage(MassachusettsHighway highway, float speed) {
		super();
		this.highway = highway;
		this.speed = speed;
	}

	private MassachusettsHighway highway;

	private float speed;

	public float getSpeed() {
		return speed;
	}

	public void setSpeed(float speed) {
		this.speed = speed;
	}

	public MassachusettsHighway getHighway() {
		return highway;
	}

	public void setHighway(MassachusettsHighway highway) {
		this.highway = highway;
	}
}
