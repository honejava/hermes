package pl.allegro.tech.hermes.consumers.consumer.result;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.offset.LightOffsetCommitter;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetQueue;
import pl.allegro.tech.hermes.consumers.consumer.Message;

public abstract class AbstractHandler {

    protected LightOffsetCommitter committer;
    protected HermesMetrics hermesMetrics;

    public AbstractHandler(LightOffsetCommitter committer, HermesMetrics hermesMetrics) {
        this.committer = committer;
        this.hermesMetrics = hermesMetrics;
    }

    protected void updateMetrics(String counterToUpdate, Message message, Subscription subscription) {
        hermesMetrics.counter(counterToUpdate, subscription.getTopicName(), subscription.getName()).inc();
        hermesMetrics.decrementInflightCounter(subscription);
        hermesMetrics.inflightTimeHistogram(subscription).update(System.currentTimeMillis() - message.getReadingTimestamp());
    }
}
