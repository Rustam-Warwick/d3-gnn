package unit;

import elements.DirectedEdge;
import elements.Feature;
import elements.Vertex;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;

import java.util.Random;

public class ElementIdTest {
    static String getRandomString(int targetStringLength){
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

    }


    @RepeatedTest(10)
    @Test
    public void testVertexWithFeatures(){
        // Vertex id
        String id = getRandomString(20);
        Vertex v = new Vertex(id);
        Assertions.assertEquals(id, v.getId());

        // Feature id
        String vertexFeatureName = getRandomString(20);
        Feature<?,?> feature = new Feature<>(vertexFeatureName, new Object());
        feature.setElement(v, true);
        Assertions.assertEquals(feature.getId(), String.format("%s%s%s%s%s", v.getId(),Feature.DELIMITER,vertexFeatureName, Feature.DELIMITER, v.getType().ordinal()));
        Assertions.assertEquals(Feature.decodeAttachedFeatureId(feature.getId()),feature.ids);

        // Non-Attributed Edge
        DirectedEdge nonAttributedEdge = new DirectedEdge(v,v);
        Assertions.assertEquals(nonAttributedEdge.getId(), String.format("%s%s%s", v.getId(), DirectedEdge.DELIMITER, v.getId()));
        Assertions.assertEquals(DirectedEdge.decodeVertexIdsAndAttribute(nonAttributedEdge.getId()), nonAttributedEdge.ids);

        // Attributed
        String attribute = getRandomString(20);
        DirectedEdge attributedEdge = new DirectedEdge(v,v, attribute);
        Assertions.assertEquals(attributedEdge.getId(), String.format("%s%s%s%s%s", v.getId(), DirectedEdge.DELIMITER, v.getId(), DirectedEdge.DELIMITER, attribute));
        Assertions.assertEquals(DirectedEdge.decodeVertexIdsAndAttribute(attributedEdge.getId()), attributedEdge.ids);

    }


}
