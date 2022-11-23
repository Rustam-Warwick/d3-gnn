package elements.enums;

/**
 * Context for copy constructor to nullify or deepCopy some fields
 * <strong>RMI</strong> used during updatable method calls. Copy all the fields except {@link elements.GraphElement::features}
 * <strong>SYNC</strong> Only copy non-halo features, and depending on the element some fields can be nullified
 */
public enum CopyContext {
    RMI, // Copy needed for RMI
    SYNC // Copy needed for Sync
}