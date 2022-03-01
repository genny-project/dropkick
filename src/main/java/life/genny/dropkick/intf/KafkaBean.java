package life.genny.dropkick.intf;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

import life.genny.qwandaq.data.BridgeSwitch;
import life.genny.qwandaq.intf.KafkaInterface;
import life.genny.qwandaq.models.GennyToken;
import life.genny.dropkick.live.data.InternalProducer;

@ApplicationScoped
public class KafkaBean implements KafkaInterface {

	static final Logger log = Logger.getLogger(KafkaBean.class);

    static Jsonb jsonb = JsonbBuilder.create();

	@Inject 
	InternalProducer producer;

	/**
	* Write a string payload to a kafka channel.
	*
	* @param channel
	* @param payload
	 */
	public void write(String channel, String payload) { 

		JsonObject event = jsonb.fromJson(payload, JsonObject.class);
		// GennyToken userToken = new GennyToken(event.getString("token"));

		if ("webcmds".equals(channel)) {
			producer.getToWebCmds().send(payload);

			// String bridgeId = BridgeSwitch.bridges.get(userToken.getUniqueId());

			// Send to a dynamic kafka channel based on the unique BridgeID in the BridgeSwitch
			// OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
			// 	.withTopic(bridgeId + "-" + channel)
			// 	.build();

			// producer.getToWebCmds().send(Message.of(event.toString()).addMetadata(metadata));

		} else if ("webdata".equals(channel)) {
			// TODO: Potentially implement dynamic kafka channel sending here as well
			producer.getToWebData().send(payload);

		} else if ("search_events".equals(channel)) {
			producer.getToSearchEvents().send(payload);

		} else {
			log.error("Producer unable to write to channel " + channel);
		}
	}
}
