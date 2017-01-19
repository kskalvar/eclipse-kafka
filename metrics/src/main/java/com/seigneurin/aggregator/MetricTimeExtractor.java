package com.seigneurin.aggregator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MetricTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record) {
        if (record.value() instanceof CounterMetric) {
            CounterMetric metric = (CounterMetric) record.value();
            return metric.getTimestampInMillis();
        } else {
            Change change = (Change) record.value();
            CounterMetric metric = change.newValue != null
                    ? (CounterMetric) change.newValue
                    : (CounterMetric) change.oldValue;
            return metric.getTimestampInMillis();
        }
    }
}
