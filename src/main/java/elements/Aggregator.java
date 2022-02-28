package elements;

public class Aggregator extends GraphElement{
    public Aggregator(String id) {
        super(id);
    }

    public Aggregator(String id, short part_id) {
        super(id, part_id);
    }

    public void addElementCallback(GraphElement element){

    }
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement){

    }

}
