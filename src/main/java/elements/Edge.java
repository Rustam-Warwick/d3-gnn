package elements;

import javax.annotation.Nullable;

public interface Edge {
    Vertex getSrc();

    String getSrcId();

    Vertex getDest();

    String getDestId();

    @Nullable
    String getAttribute();
}
