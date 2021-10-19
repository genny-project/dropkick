package org.acme.kafka.streams.aggregator.model;

import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class QDataMessageObject {
	
	private static final Logger log = Logger.getLogger(QDataMessageObject.class);
	 public int id;
    public String dataStr;
    
    public QDataMessageObject() {}

    public QDataMessageObject(int id,String strData) {
    	this.id = id;
    	this.dataStr = strData;
        log.info("QDataMessage:Str = "+id+" "+this.dataStr);
    
    }
}
