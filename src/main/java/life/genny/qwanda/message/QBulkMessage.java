package life.genny.qwanda.message;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.quarkus.runtime.annotations.RegisterForReflection;


@RegisterForReflection
public class QBulkMessage implements Serializable {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	
	private String token;

	
	private String data_type = QBulkMessage.class.getSimpleName();

	
	private QDataBaseEntityMessage[] messages;

	
	private QDataAskMessage[] asks;

	
	private String[] recipientCodeArray;

	public QBulkMessage() {
		messages = new QDataBaseEntityMessage[0];
		asks = new QDataAskMessage[0];
	}

	public QBulkMessage(QDataBaseEntityMessage[] qMessages) {
		this.messages = qMessages;
		asks = new QDataAskMessage[0];
	}

	public QBulkMessage(QDataAskMessage[] qAskMessages) {
		this.asks = qAskMessages;
		messages = new QDataBaseEntityMessage[0];
	}

	public QBulkMessage(QDataBaseEntityMessage qMessage) {
		messages = new QDataBaseEntityMessage[1];
		this.messages[0] = qMessage;
		asks = new QDataAskMessage[0];
	}

	public QBulkMessage(QDataAskMessage qAsk) {
		asks = new QDataAskMessage[1];
		this.asks[0] = qAsk;
		messages = new QDataBaseEntityMessage[0];
	}

	public QBulkMessage(List<QDataBaseEntityMessage> qMessages) {
		this.messages = new QDataBaseEntityMessage[qMessages.size()];
		this.messages = qMessages.toArray(this.messages);
	}

	public QBulkMessage(List<QDataBaseEntityMessage> qMessages, List<QDataAskMessage> qAsks) {
		this.messages = new QDataBaseEntityMessage[qMessages.size()];
		this.messages = qMessages.toArray(this.messages);
		this.asks = new QDataAskMessage[qAsks.size()];
		this.asks = qAsks.toArray(this.asks);
	}

	public void add(QDataBaseEntityMessage[] qMessageArray) {
		int newSize = ((messages == null) ? 0 : ((messages.length))) + qMessageArray.length;
		QDataBaseEntityMessage[] extended = new QDataBaseEntityMessage[newSize];

		System.arraycopy(qMessageArray, 0, extended, messages.length, qMessageArray.length);

		System.arraycopy(messages, 0, extended, 0, messages.length);
		setMessages(extended);
	}

	public void add(QDataAskMessage[] qAskArray) {
		int newSize = ((asks == null) ? 0 : ((asks.length))) + qAskArray.length;
		QDataAskMessage[] extended = new QDataAskMessage[newSize];

		System.arraycopy(qAskArray, 0, extended, asks.length, qAskArray.length);

		System.arraycopy(asks, 0, extended, 0, asks.length);
		setAsks(extended);
	}

	public void add(List<QDataBaseEntityMessage> qMessageList) {
		int newSize = ((messages == null) ? 0 : ((messages.length))) + qMessageList.size();
		QDataBaseEntityMessage[] extended = new QDataBaseEntityMessage[newSize];

		for (int index = messages.length; index < newSize; index++) {
			extended[index] = qMessageList.get(index - messages.length);
		}

		System.arraycopy(messages, 0, extended, 0, messages.length);
		setMessages(extended);

	}

	public void addAsks(List<QDataAskMessage> qAskList) {
		int newSize = ((asks == null) ? 0 : ((asks.length))) + qAskList.size();

		QDataAskMessage[] extended = new QDataAskMessage[newSize];

		for (int index = asks.length; index < newSize; index++) {
			extended[index] = qAskList.get(index - asks.length);
		}

		System.arraycopy(asks, 0, extended, 0, asks.length);
		setAsks(extended);

	}

	public void add(QDataBaseEntityMessage qMessage) {
	//	if (qMessage.getItems().length > 0) {
			Set<QDataBaseEntityMessage> set = new HashSet<QDataBaseEntityMessage>(Arrays.asList(this.messages));
			set.add(qMessage);
			this.messages = new QDataBaseEntityMessage[set.size()];
			set.toArray(this.messages);
	//	}
		int newSize = ((messages==null)?0:((messages.length)))+1;

//	    QDataBaseEntityMessage[] extended = new QDataBaseEntityMessage[newSize];
//
//		 extended[newSize-1] = qMessage;
//
//	    System.arraycopy(messages, 0, extended, 0, messages.length);
//	    setMessages(extended);
	}

	public void add(QDataAskMessage qMessage) {
		if (qMessage.getItems().length > 0) {
			Set<QDataAskMessage> set = new HashSet<QDataAskMessage>(Arrays.asList(this.asks));
			set.add(qMessage);
			this.asks = new QDataAskMessage[set.size()];
			set.toArray(this.asks);
		}
//		int newSize = ((asks==null)?0:((asks.length)))+1;
//	    QDataAskMessage[] extended = new QDataAskMessage[newSize];
//
//		 extended[newSize-1] = qMessage;
//
//	    System.arraycopy(asks, 0, extended, 0, asks.length);
//	    setAsks(extended);
	}

	public void add(QBulkMessage qBulkMessage) {
		if ((qBulkMessage.getAsks() != null)) {
			this.add(qBulkMessage.getAsks());
		}

		if ((qBulkMessage.getMessages() != null) && (qBulkMessage.getMessages().length > 0)) {
			this.add(qBulkMessage.getMessages());
		}
	}

	/**
	 * @return the messages
	 */
	public QDataBaseEntityMessage[] getMessages() {
		return messages;
	}

	/**
	 * @param messages the messages to set
	 */
	public void setMessages(QDataBaseEntityMessage[] messages) {
		this.messages = messages;
	}

	/**
	 * @return the asks
	 */
	public QDataAskMessage[] getAsks() {
		return asks;
	}

	/**
	 * @param asks the asks to set
	 */
	public void setAsks(QDataAskMessage[] asks) {
		this.asks = asks;
	}

	/**
	 * @return the token
	 */
	public String getToken() {
		return token;
	}

	/**
	 * @param token the token to set
	 */
	public void setToken(String token) {
		this.token = token;
	}

	/**
	 * @return the data_type
	 */
	public String getData_type() {
		return data_type;
	}

	/**
	 * @param data_type the data_type to set
	 */
	public void setData_type(String data_type) {
		this.data_type = data_type;
	}

	/**
	 * @return the recipientCodeArray
	 */
	public String[] getRecipientCodeArray() {
		return recipientCodeArray;
	}

	/**
	 * @param recipientCodeArray the recipientCodeArray to set
	 */
	public void setRecipientCodeArray(String[] recipientCodeArray) {
		this.recipientCodeArray = recipientCodeArray;
	}

	@Override
	public String toString() {
		int messageCount = (messages == null) ? 0 : messages.length;
		int asksCount = (asks == null) ? 0 : asks.length;
		return "QBulkMessage [" + (data_type != null ? "data_type=" + data_type : "") + " QDataBaseEntityMsgs= "
				+ messageCount + " AskMsgs=" + asksCount + "]";
	}

}
