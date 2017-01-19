# Aggregated metrics

We assume `$KAFKA_HOME/bin/` is in your path. If not already the case:

```
export PATH=$PATH:$KAFKA_HOME/bin/
```

Create topics:

```
kafka-topics --zookeeper localhost:2181 --create --topic metrics --partitions 1 --replication-factor 1

kafka-topics --zookeeper localhost:2181 --create --topic metrics-agg --partitions 1 --replication-factor 1
```
