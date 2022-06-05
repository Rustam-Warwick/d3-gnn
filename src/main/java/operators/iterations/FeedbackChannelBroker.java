package operators.iterations;

import org.apache.flink.statefun.flink.core.feedback.SubtaskFeedbackKey;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HandOffChannelBroker.
 *
 * <p>It is used together with the co-location constrain so that two tasks can access the same
 * "hand-off" channel, and communicate directly (not via the network stack) by simply passing
 * references in one direction.
 *
 * <p>To obtain a feedback channel one must first obtain an {@link SubtaskFeedbackKey} and simply
 * call {@link #get()}. A channel is removed from this broker on a call to {@link
 * FeedbackChannel#close()}.
 */
public final class FeedbackChannelBroker {

    private static final FeedbackChannelBroker INSTANCE = new FeedbackChannelBroker();

    private final ConcurrentHashMap<SubtaskFeedbackKey<?>, FeedbackChannel<?>> channels =
            new ConcurrentHashMap<>();

    public static FeedbackChannelBroker get() {
        return INSTANCE;
    }

    private static <V> FeedbackChannel<V> newChannel(SubtaskFeedbackKey<V> key) {
        return new FeedbackChannel<>(key);
    }

    @SuppressWarnings({"unchecked"})
    public <V> FeedbackChannel<V> getChannel(SubtaskFeedbackKey<V> key) {
        Objects.requireNonNull(key);
        FeedbackChannel<?> channel = channels.computeIfAbsent(key, FeedbackChannelBroker::newChannel);
        return (FeedbackChannel<V>) channel;
    }

    @SuppressWarnings("resource")
    void removeChannel(SubtaskFeedbackKey<?> key) {
        channels.remove(key);
    }
}
