package pl.allegro.tech.hermes.consumers.consumer.receiver.kafka;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.kafka.KafkaTopic;
import pl.allegro.tech.hermes.common.kafka.KafkaTopics;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.common.message.wrapper.MessageContentWrapper;
import pl.allegro.tech.hermes.common.message.wrapper.UnsupportedContentTypeException;
import pl.allegro.tech.hermes.common.message.wrapper.UnwrappedMessageContent;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceivingTimeoutException;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaRepository;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KafkaSingleThreadedMessageReceiver implements MessageReceiver {

    private final int readQueueCapacity = 1000;
    private final long readTimeout = 500;

    private KafkaConsumer<byte[], byte[]> consumer;
    private final MessageContentWrapper messageContentWrapper;
    private final SchemaRepository schemaRepository;
    private final Clock clock;

    private final BlockingQueue<Message> readQueue;

    private Topic topic;
    private volatile Subscription subscription;

    private Map<String, KafkaTopic> topics;


    public KafkaSingleThreadedMessageReceiver(KafkaConsumer<byte[], byte[]> consumer,
                                              MessageContentWrapper messageContentWrapper,
                                              SchemaRepository schemaRepository,
                                              KafkaNamesMapper kafkaNamesMapper,
                                              Topic topic,
                                              Subscription subscription,
                                              Clock clock) {
        this.topic = topic;
        this.subscription = subscription;
        this.topics = getKafkaTopics(topic, kafkaNamesMapper).stream()
                .collect(Collectors.toMap(t -> t.name().asString(), Function.identity()));
        this.consumer = consumer;
        this.messageContentWrapper = messageContentWrapper;
        this.schemaRepository = schemaRepository;
        this.clock = clock;
        this.readQueue = new ArrayBlockingQueue<Message>(readQueueCapacity);
        this.consumer.subscribe(topics.keySet());
    }

    private Collection<KafkaTopic> getKafkaTopics(Topic topic, KafkaNamesMapper kafkaNamesMapper) {
        KafkaTopics kafkaTopics = kafkaNamesMapper.toKafkaTopics(topic);
        ImmutableList.Builder<KafkaTopic> topicsBuilder = new ImmutableList.Builder<KafkaTopic>().add(kafkaTopics.getPrimary());
        kafkaTopics.getSecondary().ifPresent(topicsBuilder::add);
        return topicsBuilder.build();
    }

    @Override
    public Message next() {
        try {
            if (readQueue.isEmpty()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                if (records.isEmpty()) throw new MessageReceivingTimeoutException("No messages received");
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    UnwrappedMessageContent unwrappedContent = getUnwrappedMessageContent(record);
                    KafkaTopic kafkaTopic = topics.get(record.topic());
                    Message message = new Message(
                            unwrappedContent.getMessageMetadata().getId(),
                            topic.getQualifiedName(),
                            unwrappedContent.getContent(),
                            kafkaTopic.contentType(),
                            unwrappedContent.getSchema(),
                            unwrappedContent.getMessageMetadata().getTimestamp(),
                            clock.millis(),
                            new PartitionOffset(kafkaTopic.name(), record.offset(), record.partition()),
                            unwrappedContent.getMessageMetadata().getExternalMetadata(),
                            subscription.getHeaders()
                    );
                    readQueue.add(message);
                }
            }
            return readQueue.poll();
        } catch (KafkaException ex) {
            throw new MessageReceivingTimeoutException("No messages received", ex);
        }
    }

    private UnwrappedMessageContent getUnwrappedMessageContent(ConsumerRecord<byte[], byte[]> message) {
        if (topic.getContentType() == ContentType.AVRO) {
            return messageContentWrapper.unwrapAvro(message.value(), topic, schemaRepository);
        } else if (topic.getContentType() == ContentType.JSON) {
            return messageContentWrapper.unwrapJson(message.value());
        }
        throw new UnsupportedContentTypeException(topic);
    }

    @Override
    public void stop() {
        consumer.close();
    }

    @Override
    public void update(Subscription newSubscription) {
        this.subscription = subscription;
    }
}
