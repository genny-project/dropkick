package life.genny.dropkick.live.data;

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

  @Inject @Channel("search_eventsout") Emitter<String> searchEvents;
  public Emitter<String> getToSearchEvents() {
    return searchEvents;
  }
  
}

