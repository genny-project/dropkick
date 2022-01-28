package life.genny.dropkick.intf;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import life.genny.qwandaq.intf.KafkaInterface;
import life.genny.dropkick.live.data.InternalProducer;

@ApplicationScoped
public class KafkaBean implements KafkaInterface {

	@Inject 
	InternalProducer producer;

	private static final Logger log = Logger.getLogger(KafkaBean.class);

	/**
	* Write a string payload to a kafka channel.
	*
	* @param channel
	* @param payload
	 */
	public void write(String channel, String payload) { 

		if ("webcmds".equals(channel)) {
			producer.getToWebCmds().send(payload);

		} else if ("webdata".equals(channel)) {
			producer.getToWebData().send(payload);

		} else if ("search_events".equals(channel)) {
			producer.getToSearchEvents().send(payload);

		} else {
			log.error("Producer unable to write to channel " + channel);
		}
	}
}
