$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --topic invoice --partitions 5 --replication-factor 3 --config segment.bytes=1000000
