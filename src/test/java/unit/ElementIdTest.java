package unit;

import elements.*;
import elements.enums.ElementType;
import helpers.utils.RandomStringGenerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Collections;

public class ElementIdTest {


    public void testAttachedFeature(GraphElement element, Feature<?, ?> feature, String featureName) {
        feature.setElement(element, true);
        Assertions.assertEquals(feature.getId(), Tuple3.of(element.getType(), element.getId(), featureName));
        Assertions.assertEquals(feature.getElement(), element);
    }

    @RepeatedTest(10)
    @Test
    public void testVertexWithFeatures() {
        // Standalone Feature
        String featureName = RandomStringGenerator.getRandomString(20);
        Feature<?, ?> feature = new Feature<>(featureName, new Object());
        Assertions.assertTrue(feature.id.f0 == ElementType.NONE && feature.id.f1 == null && feature.getName().equals(featureName));
        Assertions.assertEquals(feature.getType(), ElementType.STANDALONE_FEATURE);

        // Vertex id
        String vertexId = RandomStringGenerator.getRandomString(20);
        Vertex v = new Vertex(vertexId);
        Assertions.assertEquals(vertexId, v.getId());
        Assertions.assertEquals(v.getType(), ElementType.VERTEX);
        testAttachedFeature(v, feature, featureName);


        // Non-Attributed Edge
        DirectedEdge nonAttributedEdge = new DirectedEdge(v, v);
        Assertions.assertEquals(nonAttributedEdge.getId(), Tuple3.of(v.getId(), v.getId(), null));
        Assertions.assertEquals(nonAttributedEdge.getType(), ElementType.EDGE);
        testAttachedFeature(nonAttributedEdge, feature, featureName);

        // Attributed Edge
        String attribute = RandomStringGenerator.getRandomString(20);
        DirectedEdge attributedEdge = new DirectedEdge(v, v, attribute);
        Assertions.assertEquals(attributedEdge.getId(), Tuple3.of(v.getId(), v.getId(), attribute));
        Assertions.assertEquals(attributedEdge.getType(), ElementType.EDGE);
        testAttachedFeature(attributedEdge, feature, featureName);

        // HyperEdge Test
        String hyperEdgeId = RandomStringGenerator.getRandomString(20);
        HyperEdge hyperEdge = new HyperEdge(hyperEdgeId, Collections.emptyList());
        Assertions.assertEquals(hyperEdge.getId(), hyperEdge.getId());
        Assertions.assertEquals(hyperEdge.getType(), ElementType.HYPEREDGE);
        testAttachedFeature(hyperEdge, feature, featureName);

    }


}
