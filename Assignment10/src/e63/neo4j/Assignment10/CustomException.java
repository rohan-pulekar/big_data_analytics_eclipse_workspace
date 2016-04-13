package e63.neo4j.Assignment10;

public class CustomException extends Exception {
	private static final long serialVersionUID = 1L;

	private String message;

	public CustomException(String message) {
		super();
		this.setMessage(message);
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
