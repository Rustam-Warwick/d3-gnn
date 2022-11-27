package unit;

import elements.*;
import elements.enums.ElementType;
import helpers.utils.RandomStringGenerator;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Collections;

public class ElementIdTest {


    public void testAttachedFeature(GraphElement element, Feature<?,?> feature, String featureName){
        feature.setElement(element, true);
        Assertions.assertEquals(feature.getId(), String.format("%s%s%s%s%s", element.getId(), Feature.DELIMITER, featureName, Feature.DELIMITER, element.getType().ordinal()));
        Assertions.assertEquals(Feature.decodeAttachedFeatureId(feature.getId()),feature.ids);
    }

    @RepeatedTest(10)
    @Test
    public void testVertexWithFeatures(){
        // Standalone Feature
        String featureName = RandomStringGenerator.getRandomString(20);
        Feature<?,?> feature = new Feature<>(featureName, new Object());
        Assertions.assertTrue(feature.ids.f0 == ElementType.NONE && feature.ids.f1 == null && feature.getName().equals(featureName));
        Assertions.assertEquals(feature.getType(), ElementType.STANDALONE_FEATURE);

        // Vertex id
        String vertexId = RandomStringGenerator.getRandomString(20);
        Vertex v = new Vertex(vertexId);
        Assertions.assertEquals(vertexId, v.getId());
        Assertions.assertEquals(v.getType(), ElementType.VERTEX);
        testAttachedFeature(v, feature, featureName);


        // Non-Attributed Edge
        DirectedEdge nonAttributedEdge = new DirectedEdge(v,v);
        Assertions.assertEquals(nonAttributedEdge.getId(), String.format("%s%s%s", v.getId(), DirectedEdge.DELIMITER, v.getId()));
        Assertions.assertEquals(DirectedEdge.decodeVertexIdsAndAttribute(nonAttributedEdge.getId()), nonAttributedEdge.ids);
        Assertions.assertEquals(nonAttributedEdge.getType(), ElementType.EDGE);
        testAttachedFeature(nonAttributedEdge, feature, featureName);

        // Attributed Edge
        String attribute = RandomStringGenerator.getRandomString(20);
        DirectedEdge attributedEdge = new DirectedEdge(v,v, attribute);
        Assertions.assertEquals(attributedEdge.getId(), String.format("%s%s%s%s%s", v.getId(), DirectedEdge.DELIMITER, v.getId(), DirectedEdge.DELIMITER, attribute));
        Assertions.assertEquals(DirectedEdge.decodeVertexIdsAndAttribute(attributedEdge.getId()), attributedEdge.ids);
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
