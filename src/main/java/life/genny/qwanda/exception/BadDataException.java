package life.genny.qwanda.exception;


@SuppressWarnings("serial")
public class BadDataException extends Exception {

	public BadDataException() {
		super();
	}

	public BadDataException(String message) {
		super(message);
	}
}