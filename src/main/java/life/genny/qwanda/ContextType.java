package life.genny.qwanda;

public enum ContextType {
	THEME("THEME"),
	UNITY("UNITY"),
	ICON("ICON"),
	ALIAS("ALIAS");

	private final String type;
	
	ContextType(String value) {
		this.type = value;
	}
	
	public String contextType() {
		return type;
	}	
}
