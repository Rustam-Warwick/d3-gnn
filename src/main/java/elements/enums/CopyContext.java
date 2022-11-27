package elements.enums;

/**
 * Context for copy constructor to nullify or deepCopy some fields
 * <strong>RMI</strong> used during updatable method calls. Copy all the fields except {@link elements.GraphElement::features}
 * <strong>SYNC</strong> Only copy non-halo features, and depending on the element some fields can be nullified
 */
public enum CopyContext {

    /**
     * Copy needed for {@link elements.Rmi} execution. Deep copy elements if necessary
     */
    RMI,
    /**
     * Copy needed for SYNC {@link elements.GraphOp} messages. Usually keeping only non-halo features and some custom stuff depending on element
     */
    SYNC,
}