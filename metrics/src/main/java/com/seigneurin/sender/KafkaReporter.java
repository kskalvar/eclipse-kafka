package com.seigneurin.sender;

import com.codahale.metrics.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KafkaReporter extends ScheduledReporter {

    private final Clock clock;
    private final String prefix;
    private final String topic;
    private final String messageKey;
    private final KafkaProducer<String, String> producer;

    private KafkaReporter(MetricRegistry registry, Clock clock, String prefix,
                          TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter,
                          String kafkaServers, String topic, String messageKey) {
        super(registry, "kafka-reporter", filter, rateUnit, durationUnit);

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.clock = clock;
        this.prefix = prefix;
        this.topic = topic;
        this.messageKey = messageKey;
        this.producer = new KafkaProducer<>(props);
    }

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        long timestamp = clock.getTime() / 1000;
        gauges.forEach((key, value) -> sendGauge(key, value, timestamp));
        counters.forEach((key, value) -> sendCounter(key, value, timestamp));
        histograms.forEach((key, value) -> sendHistogram(key, value, timestamp));
        meters.forEach((key, value) -> sendMeter(key, value, timestamp));
        timers.forEach((key, value) -> sendTimer(key, value, timestamp));
    }

    private void sendTimer(String name, Timer timer, long timestamp) {
        Snapshot snapshot = timer.getSnapshot();
        String value = String.format("{ " +
                        "\"type\":\"TIMER\", " +
                        "\"name\":\"%s\", " +
                        "\"count\":%d, " +
                        "\"min\":%f, " +
                        "\"max\":%f, " +
                        "\"mean\":%f, " +
                        "\"stddev\":%f, " +
                        "\"median\":%f, " +
                        "\" p75\":%f, " +
                        "\"p95\":%f, " +
                        "\"p98\":%f, " +
                        "\"p99\":%f, " +
                        "\"p999\":%f, " +
                        "\"mean_rate\":%f, " +
                        "\"m1\":%f, " +
                        "\"m5\":%f, " +
                        "\"m15\":%f, " +
                        "\"rate_unit\":\"%s\", " +
                        "\"duration_unit\":\"%s\", " +
                        "\"timestamp\":\"%d\" " +
                        "}",
                prefix(name),
                timer.getCount(),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit(),
                timestamp);
        send(name, value);
    }

    private void sendMeter(String name, Meter meter, long timestamp) {
        String value = String.format("{ " +
                        "\"type\":\"METER\", " +
                        "\"name\":\"%s\", " +
                        "\"count\":%d, " +
                        "\"mean_rate\":%f, " +
                        "\"m1\":%f, " +
                        "\"m5\":%f, " +
                        "\"m15\":%f, " +
                        "\"rate_unit\":\"%s\", " +
                        "\"timestamp\":\"%d\" " +
                        "}",
                prefix(name),
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit(),
                timestamp);
        send(name, value);
    }

    private void sendHistogram(String name, Histogram histogram, long timestamp) {
        Snapshot snapshot = histogram.getSnapshot();
        String value = String.format("{ " +
                        "\"type\":\"HISTOGRAM\", " +
                        "\"name\":\"%s\", " +
                        "\"count\":%d, " +
                        "\"min\":%d, " +
                        "\"max\":%d, " +
                        "\"mean\":%f, " +
                        "\"stddev\":%f, " +
                        "\"median\":%f, " +
                        "\"p75\":%f, " +
                        "\"p95\":%f, " +
                        "\"p98\":%f, " +
                        "\"p99\":%f, " +
                        "\"p999\":%f, " +
                        "\"timestamp\":\"%d\" " +
                        "}",
                prefix(name),
                histogram.getCount(),
                snapshot.getMin(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile(),
                timestamp);
        send(name, value);
    }

    private void sendCounter(String name, Counter counter, long timestamp) {
        String value = String.format("{ " +
                        "\"type\":\"COUNTER\", " +
                        "\"name\":\"%s\", " +
                        "\"count\":%d, " +
                        "\"timestamp\":\"%d\" " +
                        "}",
                prefix(name),
                counter.getCount(),
                timestamp);
        send(name, value);
    }

    private void sendGauge(String name, Gauge gauge, long timestamp) {
        String value = String.format("{ " +
                        "\"type\":\"GAUGE\", " +
                        "\"name\":\"%s\", " +
                        "\"value\":\"%s\", " +
                        "\"timestamp\":\"%d\" " +
                        "}",
                prefix(name),
                gauge.getValue(),
                timestamp);
        send(name, value);
    }

    private void send(String name, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageKey + ":" + name, value);
        producer.send(record);
    }

    private String prefix(String... components) {
        return MetricRegistry.name(prefix, components);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private String kafkaServers;
        private String topic;
        private String messageKey;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.kafkaServers = "localhost:9092";
            this.topic = "metrics";
            this.messageKey = "app-" + UUID.randomUUID().toString();
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder toServers(String kafkaServers) {
            this.kafkaServers = kafkaServers;
            return this;
        }

        public Builder toTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder messageKey(String messageKey) {
            this.messageKey = messageKey;
            return this;
        }

        public KafkaReporter build() {
            return new KafkaReporter(registry, clock, prefix,
                    rateUnit, durationUnit, filter,
                    kafkaServers, topic, messageKey);
        }
    }
}
