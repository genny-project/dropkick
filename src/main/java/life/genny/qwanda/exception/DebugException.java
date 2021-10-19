package life.genny.qwanda.exception;

// This exception is used to help debug errors that pop up in the log to give us a trace

@SuppressWarnings("serial")
public class DebugException extends Exception {

	public DebugException() {
		super();
	}

	public DebugException(String errorMessage) {
		super(errorMessage);
	}
	
	public DebugException(String errorMessage, Throwable err) {
	    super(errorMessage, err);
	}
}