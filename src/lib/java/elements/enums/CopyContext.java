package elements.enums;

/**
 * Context for copy constructor to nullify or deepCopy some fields
 * <strong>RMI</strong> used during updatable method calls. Copy all the fields except {@link elements.GraphElement::features}
 * <strong>SYNC</strong> Only copy non-halo features, and depending on the element some fields can be nullified
 */
public enum CopyContext {

    /**
     * Copy needed for {@link elements.Rmi} execution. Depends on the element how deep the copy should be.
     * Does not recursively copy the features!
     * <p> Ex: For in-place features just a regular shallow copy </p>
     */
    RMI,

    /**
     * Copy needed for SYNC {@link elements.GraphOp} messages.
     * Also, recursively copy the non-halo features of the given element
     */
    SYNC,

    /**
     * Simple shallow copy. All the fields are directly copied except the nested features
     * It is left null
     */
    SHALLOW

}