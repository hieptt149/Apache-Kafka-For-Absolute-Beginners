package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class DispatcherDemo {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        KafkaProducer<Integer, String> producer = null;
        Properties properties = new Properties();
        try {
            InputStream inputStream = Files.newInputStream(Paths.get(AppConfigs.kafkaConfigFileLocation));
            properties.load(inputStream);
            properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(properties);
            Thread[] dispatchers = new Thread[AppConfigs.eventFiles.length];
            logger.info("Starting dispatcher threads...");
            for (int i = 0; i < AppConfigs.eventFiles.length; i++) {
                dispatchers[i] = new Thread(new Dispatcher(AppConfigs.eventFiles[i], AppConfigs.topicName, producer));
                dispatchers[i].start();
            }
            for (Thread thread : dispatchers) {
                thread.join();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            logger.info("Finished dispatcher demo");
            if (producer != null) {
                producer.close();
            }
        }
    }
}
