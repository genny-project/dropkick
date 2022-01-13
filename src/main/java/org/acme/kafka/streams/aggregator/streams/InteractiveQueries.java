package org.acme.kafka.streams.aggregator.streams;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.WeatherStationData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class InteractiveQueries {
	
	private static final Logger log = Logger.getLogger(InteractiveQueries.class);

	String host;

	
    @Inject
    KafkaStreams streams;
    
    public InteractiveQueries()
    {
    	host =  ConfigProvider.getConfig().getValue("my.hostname", String.class);//InetAddress.getLocalHost().getHostName();
    }

 
    private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
        while (true) {
            try {
                return null;//streams.store(TopologyProducer.WEATHER_STATIONS_STORE, QueryableStoreTypes.keyValueStore());
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
    
    public GetWeatherStationDataResult getWeatherStationData(int id) {
        StreamsMetadata metadata = null; //streams.metadataForKey(                  
                //TopologyProducer.WEATHER_STATIONS_STORE,
                //id,
                //Serdes.Integer().serializer()
        //);

        if (metadata == null || metadata == StreamsMetadata.NOT_AVAILABLE) {
            log.warn("Found no metadata for key "+id);
            return GetWeatherStationDataResult.notFound();
        }
        else if (metadata.host().equals(host)) {                            
            log.info("Found data for key "+id+" locally");
            Aggregation result = getWeatherStationStore().get(id);

            if (result != null) {
                return GetWeatherStationDataResult.found(WeatherStationData.from(result));
            }
            else {
                return GetWeatherStationDataResult.notFound();
            }
        }
        else {                                                              
            log.info(
                "Found data for key "+id+" on remote host "+metadata.host()+":"+metadata.port()
            );
            return GetWeatherStationDataResult.foundRemotely(metadata.host(), metadata.port());
        }
    }

    public List<PipelineMetadata> getMetaData() {                           
        return streams.allMetadataForStore(TopologyProducer.WEATHER_STATIONS_STORE)
                .stream()
                .map(m -> new PipelineMetadata(
                        m.hostInfo().host() + ":" + m.hostInfo().port(),
                        m.topicPartitions()
                            .stream()
                            .map(TopicPartition::toString)
                            .collect(Collectors.toSet()))
                )
                .collect(Collectors.toList());
    }
}
