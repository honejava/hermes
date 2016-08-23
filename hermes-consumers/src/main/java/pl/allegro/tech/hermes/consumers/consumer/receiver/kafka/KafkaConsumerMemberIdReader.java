package pl.allegro.tech.hermes.consumers.consumer.receiver.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

public class KafkaConsumerMemberIdReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerMemberIdReader.class);

    public static String read(Consumer consumer) {
        if (consumer instanceof KafkaConsumer) {
            KafkaConsumer kafkaConsumer = (KafkaConsumer) consumer;
            try {
                Field field = KafkaConsumer.class.getDeclaredField("coordinator");
                field.setAccessible(true);
                AbstractCoordinator coordinator = (AbstractCoordinator) field.get(kafkaConsumer);
                Field memberField = AbstractCoordinator.class.getDeclaredField("memberId");
                memberField.setAccessible(true);
                return (String) memberField.get(coordinator);
            } catch (Exception e) {
                LOGGER.error("Could not read member-id from consumer.", e);
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("sry bro");
    }
}
