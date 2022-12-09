package typeinfo.recursivepojoinfo;

/**
 * Implement this class if you need to listen to serialization events
 */
public interface DeSerializationListener {
    default void onDeserialized() {
        // Do something after de-serialization
    }
}
