package com.seigneurin.aggregator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.io.IOException;
import java.util.Properties;

public class MetricsAggregator {

    public static void main(String[] args) throws IOException {

        String zookeeperServer = args[0];
        String kafkaServer = args[1];
        String rawMetricsTopic = args[2];
        String aggMetricsTopic = args[3];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metrics-aggregator");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServer);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1 * 1000);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MetricTimeExtractor.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        StreamsConfig config = new StreamsConfig(props);

        Serde<String> appIdSerde = Serdes.String();
        Serde<String> metricNameSerde = Serdes.String();
        CounterMetricSerde metricSerde = new CounterMetricSerde();

        KStreamBuilder builder = new KStreamBuilder();

        // --- First topology

        KTable<String, CounterMetric> metricsStream = builder.table(appIdSerde, metricSerde, rawMetricsTopic, "raw-metrics");

        // FIXME
        metricsStream.foreach((key, value) -> System.out.println("RECEIVED - " + key + " --> " + value));

        KStream<String, CounterMetric> metricValueStream = metricsStream
                .groupBy((key, value) -> new KeyValue<>(value.getName(), value), metricNameSerde, metricSerde)
                .reduce(CounterMetric::add, CounterMetric::subtract, "aggregates")
                .toStream();

        // FIXME
        metricValueStream.foreach((key, value) -> System.out.println("OUTPUT - " + key + " --> " + value));

        metricValueStream.to(metricNameSerde, metricSerde, aggMetricsTopic);

        // --- Second topology

        GraphiteReporter graphite = GraphiteReporter.builder()
                .hostname("localhost")
                .port(2003)
                .build();

        KStream<String, CounterMetric> aggMetricsStream = builder.stream(metricNameSerde, metricSerde, aggMetricsTopic);
        aggMetricsStream.foreach((key, metric) -> graphite.send(metric));

        // ---

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
