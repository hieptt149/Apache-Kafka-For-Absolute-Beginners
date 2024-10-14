$KAFKA_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic hello-producer-2 --partitions 5 --replication-factor 3 --config min.insync.replicas=2
