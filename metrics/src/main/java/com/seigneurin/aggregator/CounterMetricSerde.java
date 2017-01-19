package com.seigneurin.aggregator;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CounterMetricSerde implements Serde<CounterMetric> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<CounterMetric> serializer() {
        return new Serializer<CounterMetric>() {

            private Gson gson = new Gson();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, CounterMetric metric) {
                JsonObject jsonObject = new JsonObject();
                jsonObject.add("type", new JsonPrimitive("METER"));
                jsonObject.add("name", new JsonPrimitive(metric.getName()));
                jsonObject.add("count", new JsonPrimitive(metric.getValue()));
                jsonObject.add("timestamp", new JsonPrimitive(metric.getTimestampInMillis()));
                String json = gson.toJson(jsonObject);
                return json.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
            }
        };
    }

    @Override
    public Deserializer<CounterMetric> deserializer() {
        return new Deserializer<CounterMetric>() {

            private JsonParser jsonParser = new JsonParser();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public CounterMetric deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                String json = new String(data, StandardCharsets.UTF_8);
                JsonObject jsonObject = jsonParser.parse(json).getAsJsonObject();
                String name = jsonObject.get("name").getAsString();
                long value = jsonObject.get("count").getAsLong();
                long timestamp = jsonObject.has("timestamp")
                        ? jsonObject.get("timestamp").getAsLong()
                        : System.currentTimeMillis();
                return new CounterMetric(name, value, timestamp);
            }

            @Override
            public void close() {
            }
        };
    }

}
