package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;

/**
 * Vertex --> {@link ReplicableGraphElement}
 */
public final class Vertex extends ReplicableGraphElement {

    public String id;

    public Vertex() {
        super();
    }

    public Vertex(String id) {
        super();
        this.id = id;
    }

    public Vertex(String id, short master) {
        super(master);
        this.id = id;
    }

    public Vertex(Vertex element, CopyContext copyContext) {
        super(element, copyContext);
        this.id = element.id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex copy(CopyContext context) {
        return new Vertex(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    @Override
    public void syncRequest(GraphElement newElement) {
        getGraphRuntimeContext().getStorage().getVertices().getFeatures(getId()).filter(false).keySet().forEach(name -> {
            if(!super.containsFeature(name)){
                getGraphRuntimeContext().getStorage().getVertices().getFeatures(getId()).get(name).setElement(this, false);
            }
        });
        super.syncRequest(newElement);
    }

    @Override
    public Feature<?, ?> getFeature(String name) {
        Feature<?,?> feature = super.getFeature(name);
        if(feature == null && getGraphRuntimeContext() != null){
            feature = getGraphRuntimeContext().getStorage().getVertices().getFeatures(getId()).get(name);
            feature.setElement(this, false);
        }
        return feature;
    }

    @Override
    public Boolean containsFeature(String name) {
        boolean contains = super.containsFeature(name);
        if(!contains && getGraphRuntimeContext() != null){
            return getGraphRuntimeContext().getStorage().getVertices().getFeatures(getId()).containsKey(name);
        }
        return contains;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.VERTEX;
    }
}
