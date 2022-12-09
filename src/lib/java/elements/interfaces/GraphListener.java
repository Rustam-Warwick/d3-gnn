package elements.interfaces;

import elements.GraphElement;

/**
 * Interface for listening to changes in the {@link storage.BaseStorage}
 */
public interface GraphListener {

    /**
     * A {@link GraphElement} has been added to storage
     */
    default void addElementCallback(GraphElement element){}

    /**
     * A {@link GraphElement} has been replaced in storage
     */
    default void updateElementCallback(GraphElement newElement, GraphElement oldElement){}

    /**
     * A {@link GraphElement} has been removed from storage
     */
    default void deleteElementCallback(GraphElement deletedElement){}

}
