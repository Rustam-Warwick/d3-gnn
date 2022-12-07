package operators.interfaces;

import elements.GraphElement;

public interface GraphListener {

    default void addElementCallback(GraphElement element){

    }

    default void updateElementCallback(GraphElement newElement, GraphElement oldElement){}

    default void deleteElementCallback(GraphElement deletedElement){}

}
