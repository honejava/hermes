package pl.allegro.tech.hermes.consumers.consumer.receiver.kafka;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.consumers.consumer.Message;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;

public class KafkaSingleThreadedMessageReceiver implements MessageReceiver {
    @Override
    public Message next() {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public void update(Subscription newSubscription) {

    }
}
