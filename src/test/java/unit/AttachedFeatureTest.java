package unit;

import elements.Feature;
import elements.Vertex;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracking the references when attaching {@link Vertex} and {@link Feature}
 */
public class AttachedFeatureTest {
    final List<Vertex> vertices = new ArrayList<>(100);

    final List<Feature<?, ?>> features = new ArrayList<>(100);

    @BeforeEach
    void populateVertices() {
        for (int i = 0; i < 100; i++) {
            vertices.add(new Vertex("testVertex"));
            features.add(new Feature<>("f", new Object()));
        }
        mingle();
    }

    void mingle() {
        for (Vertex vertex : vertices) {
            for (Feature<?, ?> feature : features) {
                feature.setElement(vertex, true);
            }
        }
    }

    @Test
    public void checkReferences() {
        for (Vertex vertex : vertices) {
            Assertions.assertNotNull(vertex.features);
            Assertions.assertEquals(vertex.features.size(), 1);
            Assertions.assertEquals(vertex.features.get(0), features.get(0));
        }
        for (Feature<?, ?> feature : features) {
            Assertions.assertNotNull(feature.ids.f0);
            Assertions.assertNotNull(feature.ids.f1);
            Assertions.assertNotNull(feature.ids.f2);
            Assertions.assertNotNull(feature.element);
            Assertions.assertEquals(feature.element, vertices.get(99));
        }
    }

}
