package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating Kafka producer...");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties);

        logger.info("Start sending messages...");
        for (int i = 0; i < AppConfigs.numEvents; i++) {
            kafkaProducer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple message - " + i));
        }
        logger.info("Finish sending messages. Closing producer");
        kafkaProducer.close();
    }
}
