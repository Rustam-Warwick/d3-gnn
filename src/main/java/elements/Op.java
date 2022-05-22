package elements;

public enum Op {
    NONE,
    COMMIT,
    REMOVE,
    SYNC,
    RMI,
    WATERMARK, // Normal Watermarks flowing forward
    WATERMARK_STATUS, // Watermark Status
    FINISH_EXECUTION // Finish the operator execution
}
