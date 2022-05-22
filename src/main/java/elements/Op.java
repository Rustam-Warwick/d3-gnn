package elements;

public enum Op {
    NONE,
    COMMIT,
    REMOVE,
    SYNC,
    RMI,
    WATERMARK, // Normal Watermarks flowing forward
    TRAIN_WATERMARK,
    FINISH_EXECUTION // Finish the operator execution
}
