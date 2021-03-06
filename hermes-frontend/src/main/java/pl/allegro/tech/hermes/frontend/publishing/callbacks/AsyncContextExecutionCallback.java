package pl.allegro.tech.hermes.frontend.publishing.callbacks;


import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.frontend.publishing.PublishingCallback;
import pl.allegro.tech.hermes.frontend.publishing.message.Message;

import javax.servlet.AsyncContext;
import java.util.Arrays;

public class AsyncContextExecutionCallback implements PublishingCallback {
    private final AsyncContext asyncContext;
    private final PublishingCallback[] callbacks;

    public AsyncContextExecutionCallback(AsyncContext asyncContext, PublishingCallback... callbacks) {
        this.asyncContext = asyncContext;
        this.callbacks = callbacks;
    }

    @Override
    public void onPublished(Message message, Topic topic) {
        asyncContext.start(() -> Arrays.stream(callbacks).forEach(c -> c.onPublished(message, topic)));
    }

    @Override
    public void onUnpublished(Message message, Topic topic, Exception exception) {
        asyncContext.start(() -> Arrays.stream(callbacks).forEach(c -> c.onUnpublished(message, topic, exception)));
    }
}
