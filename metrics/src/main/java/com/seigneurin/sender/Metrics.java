package com.seigneurin.sender;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Metrics {

    private static final Logger logger = LoggerFactory.getLogger(Metrics.class);

    public static MetricRegistry registry = new MetricRegistry();

    public static void init(String kafkaServer, String metricsAppId) {

        Slf4jReporter.forRegistry(registry)
                .outputTo(LoggerFactory.getLogger("com.capitalone.metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()
                .start(5, TimeUnit.SECONDS);

        GraphiteReporter.forRegistry(registry)
                .prefixedWith("serviceintegrator." + metricsAppId)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(new Graphite("localhost", 2003))
                .start(1, TimeUnit.SECONDS);

        KafkaReporter.forRegistry(registry)
                .prefixedWith("serviceintegrator")
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .toServers(kafkaServer)
                .toTopic("metrics")
                .messageKey(metricsAppId)
                .build()
                .start(1, TimeUnit.SECONDS);
    }

}
