package org.acme.kafka.streams.aggregator.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

/**
 * InternalProducer --- Kafka smalltye producer objects to send to internal consumers backends
 * such as wildfly-rulesservice. 
 *
 * @author    hello@gada.io
 *
 */
@ApplicationScoped
public class InternalProducer {



  @Inject @Channel("webcmds") Emitter<String> webcmds;
  public Emitter<String> getToWebCmds() {
    return webcmds;
  }

  @Inject @Channel("webdata") Emitter<String> webdata;
  public Emitter<String> getToWebData() {
    return webdata;
  }

}

