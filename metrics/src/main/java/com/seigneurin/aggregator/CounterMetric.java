package com.seigneurin.aggregator;

public class CounterMetric {

    private String name;
    private long value;
    private long timestampInMillis;

    public CounterMetric(String name, long value, long timestampInMillis) {
        this.name = name;
        this.value = value;
        this.timestampInMillis = timestampInMillis;
    }

    public static CounterMetric add(CounterMetric metric1, CounterMetric metric2) {
        if (metric1.name.equals(metric2.name) == false) {
            throw new RuntimeException(String.format("Cannot add metrics '%s' and '%s'", metric1.name, metric2.name));
        }
        return new CounterMetric(metric1.name,
                metric1.value + metric2.value,
                metric2.timestampInMillis);
    }

    public static CounterMetric subtract(CounterMetric metric1, CounterMetric metric2) {
        if (metric1 == null)
            return metric2;
        if (metric2 == null)
            return metric1;
        return new CounterMetric(metric1.name,
                metric1.value - metric2.value,
                metric1.timestampInMillis);
    }

    public String getName() {
        return name;
    }

    public long getValue() {
        return value;
    }

    public long getTimestampInMillis() {
        return timestampInMillis;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "CounterMetric: " + name + " = " + value + " @" + timestampInMillis;
    }
}
