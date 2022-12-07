package org.apache.flink.streaming.api.operators.iteration;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton Broker Pattern for the {@link IterationChannel}
 */
public class IterationChannelBroker {
    protected static final IterationChannelBroker INSTANCE = new IterationChannelBroker();

    /**
     * List of {@link IterationChannel} defined by Tuple3<JobID, IterationID, AttemptID>
     */
    protected ConcurrentHashMap<IterationChannelKey, IterationChannel<?>> channels = new ConcurrentHashMap<>();

    /**
     * Return singleton broker
     */
    public static IterationChannelBroker getBroker() {
        return INSTANCE;
    }

    /**
     * Get {@link IterationChannel} or create and get
     */
    public <T> IterationChannel<T> getIterationChannel(IterationChannelKey iterationChannelKey){
        channels.computeIfAbsent(iterationChannelKey, (ignored)->new IterationChannel<T>(iterationChannelKey));
        return (IterationChannel<T>) channels.get(iterationChannelKey);
    }

    /**
     * Remove this iteration channel from the map
     */
    public void removeChannel(IterationChannelKey channelID){
        channels.remove(channelID);
    }
}
