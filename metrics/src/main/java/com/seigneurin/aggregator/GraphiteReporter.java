package com.seigneurin.aggregator;

import com.codahale.metrics.graphite.Graphite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GraphiteReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteReporter.class);

    private final Graphite graphite;

    public GraphiteReporter(String hostname, int port) {
        this.graphite = new Graphite(hostname, port);
    }

    public void send(CounterMetric metric) {
        try {
            if (!graphite.isConnected()) {
                graphite.connect();
            }
            graphite.send(metric.getName() + ".count", Long.toString(metric.getValue()), metric.getTimestampInMillis());
            graphite.flush();
        } catch (IOException e) {
            LOGGER.warn("Unable to report to Graphite", graphite, e);
            try {
                graphite.close();
            } catch (IOException e1) {
                LOGGER.warn("Error closing Graphite", graphite, e);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String hostname;
        private int port;

        private Builder() {
            this.hostname = "localhost";
            this.port = 2003;
        }

        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public GraphiteReporter build() {
            return new GraphiteReporter(hostname, port);
        }
    }

}
