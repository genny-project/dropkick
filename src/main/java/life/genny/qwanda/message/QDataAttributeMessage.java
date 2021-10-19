package life.genny.qwanda.message;



import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.attribute.Attribute;

@RegisterForReflection
public class QDataAttributeMessage extends QDataMessage{

	private static final long serialVersionUID = 1L;
	
	public QDataAttributeMessage() {
		super(DATATYPE_ATTRIBUTE);
	}

	private Attribute[] items;
	private static final String DATATYPE_ATTRIBUTE = Attribute.class.getSimpleName();

	public QDataAttributeMessage(Attribute[] items) {
		super(DATATYPE_ATTRIBUTE);
		setItems(items);
	}

	public Attribute[] getItems() {
		return items;
	}

	public void setItems(Attribute[] items) {
		this.items = items;
	}

}
