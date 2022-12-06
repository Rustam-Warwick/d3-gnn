package storage;

import elements.*;
import elements.enums.CacheFeatureContext;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import operators.interfaces.RichGraphElement;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Base Class for all storage Engines
 *
 * @implNote Subclassses extendind from this class should not care about foreign keys. All GraphElements should be stored rather independent from each other
 * @implNote This is done so that late events are handled correctly, so all the logic is withing the specific graph element
 * @implNote However, do check for redundancy is create methods.
 */
abstract public class BaseStorage implements Serializable, RichGraphElement {
    /**
     * Logger
     */
    protected static Logger LOG = LoggerFactory.getLogger(BaseStorage.class);

    // ------------------------ ABSTRACT METHODS -------------------------------------


    public abstract boolean addAttachedFeature(Feature<?, ?> feature);

    public abstract boolean addStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean addVertex(Vertex vertex);

    public abstract boolean addEdge(DirectedEdge directedEdge);

    public abstract boolean addHyperEdge(HyperEdge hyperEdge);

    public abstract boolean updateAttachedFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateStandaloneFeature(Feature<?, ?> feature, Feature<?, ?> memento);

    public abstract boolean updateVertex(Vertex vertex, Vertex memento);

    public abstract boolean updateEdge(DirectedEdge directedEdge, DirectedEdge memento);

    public abstract boolean updateHyperEdge(HyperEdge hyperEdge, HyperEdge memento);

    public abstract boolean deleteAttachedFeature(Feature<?, ?> feature);

    public abstract boolean deleteStandaloneFeature(Feature<?, ?> feature);

    public abstract boolean deleteVertex(Vertex vertex);

    public abstract boolean deleteEdge(DirectedEdge directedEdge);

    public abstract boolean deleteHyperEdge(HyperEdge hyperEdge);

    @Nullable
    public abstract Vertex getVertex(String vertexId);

    public abstract Iterable<Vertex> getVertices();

    @Nullable
    public abstract DirectedEdge getEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId);

    public abstract Iterable<DirectedEdge> getEdges(String srcId, String destId);

    public abstract Iterable<DirectedEdge> getIncidentEdges(Vertex vertex, EdgeType edge_type);

    public abstract HyperEdge getHyperEdge(String hyperEdgeId);

    public abstract Iterable<HyperEdge> getIncidentHyperEdges(Vertex vertex);

    @Nullable
    public abstract Feature<?, ?> getAttachedFeature(ElementType attachedType, String attachedId, String featureName, @Nullable String featureId);

    @Nullable
    public abstract Feature<?, ?> getStandaloneFeature(String featureName);

    public abstract boolean containsVertex(String vertexId);

    public abstract boolean containsAttachedFeature(ElementType attachedType, String attachedId, String featureName, @Nullable String featureId);

    public abstract boolean containsStandaloneFeature(String featureName);

    public abstract boolean containsEdge(String srcId, String destId, @Nullable String attributeId, @Nullable String edgeId);

    public abstract boolean containsHyperEdge(String hyperEdgeId);

    /**
     * Given a {@link GraphElement} add all its available {@link Feature}
     *
     * @implNote This method is only called in <code>sync()</code> {@link GraphElement}
     */
    public abstract void cacheFeatures(GraphElement element, CacheFeatureContext context);


    // -------------------------- BASE STORAGE HELPER METHODS ------------------------------

    /**
     * Do elements need to delay Tensors on serialization, {@link Feature} having an {@link ai.djl.ndarray.NDArray} in them should consider delaying if storage requires so.
     */
    public boolean needsTensorDelay() {
        return true;
    }

    // --------------------------- MAPPER & HELPER METHODS -------------------------

    public boolean addElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return this.addVertex((Vertex) element);
            case EDGE:
                return this.addEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return addAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return addStandaloneFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return this.addHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    public boolean deleteElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return this.deleteVertex((Vertex) element);
            case EDGE:
                return this.deleteEdge((DirectedEdge) element);
            case ATTACHED_FEATURE:
                return this.deleteAttachedFeature((Feature<?, ?>) element);
            case STANDALONE_FEATURE:
                return this.deleteStandaloneFeature((Feature<?, ?>) element);
            case HYPEREDGE:
                return this.deleteHyperEdge((HyperEdge) element);
            default:
                return false;
        }
    }

    public boolean updateElement(GraphElement element, GraphElement memento) {
        switch (element.getType()) {
            case VERTEX:
                return this.updateVertex((Vertex) element, (Vertex) memento);
            case EDGE:
                return this.updateEdge((DirectedEdge) element, (DirectedEdge) element);
            case ATTACHED_FEATURE:
                return this.updateAttachedFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case STANDALONE_FEATURE:
                return this.updateStandaloneFeature((Feature<?, ?>) element, (Feature<?, ?>) memento);
            case HYPEREDGE:
                return this.updateHyperEdge((HyperEdge) element, (HyperEdge) memento);
            default:
                return false;
        }
    }

    public boolean containsElement(String id, ElementType type) {
        switch (type) {
            case VERTEX:
                return containsVertex(id);
            case EDGE:
                Tuple3<String, String, String> ids = DirectedEdge.decodeVertexIdsAndAttribute(id);
                return containsEdge(ids.f0, ids.f1, ids.f2, id);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, String, String> tmp = Feature.decodeAttachedFeatureId(id);
                return containsAttachedFeature(tmp.f0, tmp.f1, tmp.f2, id);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature(id);
            case HYPEREDGE:
                return containsHyperEdge(id);
            default:
                return false;
        }
    }

    public GraphElement getElement(String id, ElementType t) {
        switch (t) {
            case VERTEX:
                return this.getVertex(id);
            case ATTACHED_FEATURE:
                Tuple3<ElementType, String, String> tmp = Feature.decodeAttachedFeatureId(id);
                return getAttachedFeature(tmp.f0, tmp.f1, tmp.f2, id);
            case STANDALONE_FEATURE:
                return getStandaloneFeature(id);
            case EDGE:
                Tuple3<String, String, String> ids = DirectedEdge.decodeVertexIdsAndAttribute(id);
                return getEdge(ids.f0, ids.f1, ids.f2, id);
            case HYPEREDGE:
                return getHyperEdge(id);
            default:
                return null;
        }
    }

    public boolean containsElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return containsVertex(element.getId());
            case ATTACHED_FEATURE:
                Feature<?, ?> tmp = (Feature<?, ?>) element;
                return containsAttachedFeature(tmp.ids.f0, tmp.ids.f1, tmp.ids.f2, null);
            case STANDALONE_FEATURE:
                return containsStandaloneFeature(element.getId());
            case EDGE:
                DirectedEdge edge = (DirectedEdge) element;
                return containsEdge(edge.getSrcId(), edge.getDestId(), edge.getAttribute(), null);
            case HYPEREDGE:
                return containsHyperEdge(element.getId());
            default:
                return false;
        }
    }

    public GraphElement getElement(GraphElement element) {
        switch (element.getType()) {
            case VERTEX:
                return getVertex(element.getId());
            case ATTACHED_FEATURE:
                Feature<?, ?> tmp = (Feature<?, ?>) element;
                return getAttachedFeature(tmp.ids.f0, tmp.ids.f1, tmp.ids.f2, null);
            case STANDALONE_FEATURE:
                return getStandaloneFeature(element.getId());
            case EDGE:
                DirectedEdge edge = (DirectedEdge) element;
                return getEdge(edge.getSrcId(), edge.getDestId(), edge.getAttribute(), null);
            case HYPEREDGE:
                return getHyperEdge(element.getId());
            default:
                return null;
        }
    }

    public final GraphElement getDummyElement(String id, ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return new Vertex(id, getRuntimeContext().getCurrentPart());
            case HYPEREDGE:
                return new HyperEdge(id, new ArrayList<>(), getRuntimeContext().getCurrentPart());
        }
        throw new IllegalStateException("Dummy element can only be created for VERTEX and HYPEREDGE");
    }

}
