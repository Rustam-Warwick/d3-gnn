package elements;

public enum Op {
    NONE,
    COMMIT,
    REMOVE,
    SYNC,
    RMI,
    WATERMARK, // Normal Watermarks flowing forward
    CHECKPOINT_BARRIER,
    OPERATOR_EVENT, // Watermark Status
    FINISH_EXECUTION // Finish the operator execution
}
