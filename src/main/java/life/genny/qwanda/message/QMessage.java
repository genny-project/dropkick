package life.genny.qwanda.message;

import java.io.Serializable;
import java.util.List;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public abstract class QMessage implements Serializable, QMessageIntf {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public enum MsgOption {
		CACHE, // cache this message as a response to a trigger event
		EXEC, // execute this
		EXEC_CACHE, // execute this AND set up as a cached response
		LOCAL, // This message (if triggered, does not need to be sent through to the back end
				// as well
		IGNORE // the front end can ignore and handling of this message (useful for testing)
	}

	@Override
	public String toString() {
		return "QMessage [msg_type=" + msg_type + "]," + option.toString();
	}


	private String msg_type;


	private String token;


	private String option = MsgOption.EXEC.toString();


	private String triggerCode; // This can be used to trigger any option

	private List<String> targetCodes;
	
	private String sourceAddress;

	private String sourceCode;

	private String targetCode;

	private String attributeCode;

	private String questionCode;

	private String message;

	private Boolean redirect;
	
	 private String[] recipientCodeArray;
	

	public String getMsg_type() {
		return msg_type;
	}

	public void setMsg_type(String msg_type) {
		this.msg_type = msg_type;
	}

	private QMessage() {
	}

	public QMessage(String msg_type) {
		this.msg_type = msg_type;
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
	 * @return the option
	 */
	public String getOption() {
		return option;
	}

	/**
	 * @param option the option to set
	 */
	public void setOption(MsgOption option) {
		this.option = option.toString();
	}

	/**
	 * @return the triggerCode
	 */
	public String getTriggerCode() {
		return triggerCode;
	}

	/**
	 * @param triggerCode the triggerCode to set
	 */
	public void setTriggerCode(String triggerCode) {
		this.triggerCode = triggerCode;
	}

	/**
	 * @return the targetCodes
	 */
	public List<String> getTargetCodes() {
		return targetCodes;
	}

	/**
	 * @param targetCodes the targetCodes to set
	 */
	public void setTargetCodes(List<String> targetCodes) {
		this.targetCodes = targetCodes;
	}

	public String getSourceAddress() {
		return sourceAddress;
	}

	public void setSourceAddress(String sourceAddress) {
		this.sourceAddress = sourceAddress;
	}

	public String getSourceCode() {
		return sourceCode;
	}

	public void setSourceCode(String sourceCode) {
		this.sourceCode = sourceCode;
	}

	public String getTargetCode() {
		return targetCode;
	}

	public void setTargetCode(String targetCode) {
		this.targetCode = targetCode;
	}

	public String getQuestionCode() {
		return questionCode;
	}

	public void setQuestionCode(String questionCode) {
		this.questionCode = questionCode;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Boolean getRedirect() {
		return redirect;
	}

	public void setRedirect(Boolean redirect) {
		this.redirect = redirect;
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

	public String getAttributeCode() {
		return attributeCode;
	}

	public void setAttributeCode(String attributeCode) {
		this.attributeCode = attributeCode;
	}
	

	
	
}
