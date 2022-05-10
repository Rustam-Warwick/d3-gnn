package elements;

public enum Op {
    NONE,
    COMMIT,
    REMOVE,
    SYNC,
    RMI,
    WATERMARK, // Normal Watermarks flowing forward
    BACK_WATERMARK // Backward Watermarks flowing backward
}
