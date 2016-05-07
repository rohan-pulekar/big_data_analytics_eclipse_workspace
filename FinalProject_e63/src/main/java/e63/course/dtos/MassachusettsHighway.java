package e63.course.dtos;

public enum MassachusettsHighway {
	I_93("I-93"), I_95("I-95"), I_90("I-90"), I_495("I-495"), ROUTE_3("Route 3"), ROUTE_6("Route 6"), ROUTE_9(
			"Route 9"), ROUTE_25("Route 25"), ROUTE_28("Route 28");

	private final String displayName;

	MassachusettsHighway(String displayName) {
		this.displayName = displayName;
	}

	public String getDisplayName() {
		return displayName;
	}

	@Override
	public String toString() {
		return displayName;
	}

}