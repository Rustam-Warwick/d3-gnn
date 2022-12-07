package org.apache.flink.streaming.api.operators.iteration;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton Broker Pattern for the {@link IterationChannel}
 */
public class IterationChannelBroker {
    protected static final IterationChannelBroker INSTANCE = new IterationChannelBroker();

    /**
     * List of {@link IterationChannel} defined by Tuple3<JobID, IterationID, AttemptID>
     */
    protected ConcurrentHashMap<Tuple3<JobID, Integer, Integer>, IterationChannel<?>> channels = new ConcurrentHashMap<>();

    /**
     * Return singleton broker
     */
    public static IterationChannelBroker getBroker() {
        return INSTANCE;
    }

    /**
     * Get {@link IterationChannel} or create and get
     */
    public <T> IterationChannel<T> getIterationChannel(Tuple3<JobID, Integer, Integer> channelID){
        channels.computeIfAbsent(channelID, (ignored)->new IterationChannel<T>(channelID));
        return (IterationChannel<T>) channels.get(channelID);
    }

    /**
     * Remove this iteration channel from the map
     */
    public void removeChannel(Tuple3<JobID, Integer, Integer> channelID){
        channels.remove(channelID);
    }
}
