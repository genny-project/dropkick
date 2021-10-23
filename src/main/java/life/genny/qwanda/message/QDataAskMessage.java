package life.genny.qwanda.message;

import java.io.Serializable;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.Ask;

@RegisterForReflection
public class QDataAskMessage extends QDataMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Ask[] items;
	private static final String DATATYPE_ASK = Ask.class.getSimpleName();

	public QDataAskMessage(Ask[] items) {
		super(DATATYPE_ASK);
//		if ((items == null)||(items.length == 0)) {
//			setItems(new Ask[0]);
//		} else {
			setItems(items);
//		}

	}
	
	public QDataAskMessage(Ask ask) {
		super(DATATYPE_ASK);
		Ask[] asks = new Ask[1];
		asks[0] = ask;
		setItems(asks);
	}

	public Ask[] getItems() {
		return this.items;
	}

	public void setItems(Ask[] asks) {
//		if ((items == null)||(items.length == 0)) {
//			this.items = new Ask[0];
//		} else {
			this.items = asks;
//		}

	}

}
