package pl.allegro.tech.hermes.consumers.consumer.receiver;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.offset.FailedToCommitOffsets;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetsToCommit;
import pl.allegro.tech.hermes.consumers.consumer.offset.SubscriptionPartitionOffset;

public interface MessageReceiver {

    Message next();

    default void stop() {}

    default void update(Subscription newSubscription) {}

    default FailedToCommitOffsets commit(OffsetsToCommit offsets) {
        return new FailedToCommitOffsets();
    };

    default void moveOffset(SubscriptionPartitionOffset offset) {};
}
