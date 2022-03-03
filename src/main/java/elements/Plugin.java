package elements;

public class Plugin extends GraphElement{
    public Plugin(){
        super();
    }
    public Plugin(String id) {
        super(id);
    }

    @Override
    public ElementType elementType() {
        return ElementType.PLUGIN;
    }

    public void addElementCallback(GraphElement element){

    }
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement){

    }
    public void open(){

    }

}
