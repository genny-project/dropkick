package org.acme.kafka.streams.aggregator.model;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;
import org.jboss.logging.Logger;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class Aggregation {
	
	private static final Logger log = Logger.getLogger(Aggregation.class);

    public int stationId;
    public String stationName;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public int count;
    public double sum;
    public double avg;
    public double mode;

    public Aggregation updateFrom(TemperatureMeasurement measurement) {
        stationId = measurement.stationId;
        stationName = measurement.stationName;
        
        count++;
        sum += measurement.value;
        avg = BigDecimal.valueOf(sum / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();

        min = Math.min(min, measurement.value);
        max = Math.max(max, measurement.value);
        mode = avg;
        log.info("Aggregation:Average = "+avg);
        return this;
    }
}
