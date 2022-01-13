package org.acme.kafka.streams.aggregator.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;


import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;

@ApplicationScoped
public class DataGenerator {
	
	private static final Logger LOG = Logger.getLogger(DataGenerator.class);
	
    private Random random = new Random();
	
	 private List<Attribute2> attribute2s = List.of(
             new Attribute2(1, "PRI_NAME","Name"),
             new Attribute2(2, "PRI_FIRSTNAME","Firstname"),
             new Attribute2(3, "PRI_LASTNAME","Last Name"),
             new Attribute2(4, "PRI_EMAIL","Email"),
             new Attribute2(5, "PRI_MOBILE","Mobile"),
             new Attribute2(6, "PRI_COUNTRY","Country"));
 
	   @Outgoing("test-data")
	    public Multi<Record<Integer, String>> generate() {
	        return Multi.createFrom().ticks().every(Duration.ofMillis(500))
	                .onOverflow().drop()
	                .map(tick -> {
	                    Attribute2 attribute2 = attribute2s.get(random.nextInt(attribute2s.size()));

	                   // LOG.infov("attribute: {0}, id: {1}", attribute.name, attribute.id);
	                    return Record.of(attribute2.id, Instant.now() + ";" + attribute2.code+":"+attribute2.name);
	                });
	    }
	 
	 
	    @Outgoing("attributes")                                          
	    public Multi<Record<Integer, String>> attributes() {
	        return Multi.createFrom().items(attribute2s.stream()
	            .map(a -> Record.of(
	                    a.id,
	                    "{ \"id\" : " + a.id +
	                    ", \"code\" : \"" + a.code + "\"" +
	                    ", \"name\" : \"" + a.name + "\" }"))
	        );
	    }



}
