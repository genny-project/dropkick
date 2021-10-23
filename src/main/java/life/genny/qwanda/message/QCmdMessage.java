package life.genny.qwanda.message;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.ContextList;

@RegisterForReflection
public class QCmdMessage extends QMessage {

	private static final String MESSAGE_TYPE = "CMD_MSG";
	
	private String cmd_type;
	
	private String code;
	
	
	private Boolean exec;
	
	
	private Boolean send;
	
	 
	private String cache;

	
	private ContextList contextList;

	public QCmdMessage(String cmd_type, String code) {
		super(MESSAGE_TYPE);
		this.code = code;
		this.cmd_type = cmd_type;
		this.send = true;
		this.exec=true;
		this.cache = null;
	}

	public String getCmd_type() {
		return cmd_type;
	}

	public void setCmd_type(String cmd_type) {
		this.cmd_type = cmd_type;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	
	
	
	/**
	 * @return the exec
	 */
	public Boolean getExec() {
		return exec;
	}

	/**
	 * @param exec the exec to set
	 */
	public void setExec(Boolean exec) {
		this.exec = exec;
	}

	/**
	 * @return the send
	 */
	public Boolean getSend() {
		return send;
	}

	/**
	 * @param send the send to set
	 */
	public void setSend(Boolean send) {
		this.send = send;
	}

	/**
	 * @return the cache
	 */
	public String getCache() {
		return cache;
	}

	/**
	 * @param cache the cache to set
	 */
	public void setCache(String cache) {
		this.cache = cache;
	}

	/**
	 * @return the contextList
	 */
	public ContextList getContextList() {
		return contextList;
	}

	/**
	 * @param contextList the contextList to set
	 */
	public void setContextList(final ContextList contextList) {
		this.contextList = contextList;
	}

}
