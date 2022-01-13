package life.genny.qwandautils;

import java.io.Serializable;

import io.quarkus.runtime.annotations.RegisterForReflection;
import life.genny.qwanda.message.QBulkMessage;
import life.genny.qwanda.message.QCmdMessage;
import life.genny.qwanda.message.QDataAskMessage;

@RegisterForReflection
public class QwandaMessage extends  QCmdMessage implements Serializable {
		
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	

	public QDataAskMessage asks;

	public QBulkMessage askData;
	
	private static final String CMD_TYPE = "CMD_BULKASK";
	private static final String CODE = "QWANDAMESSAGE";

	
	public QwandaMessage() {
		super(CMD_TYPE, CODE);
		
	}

	
}