package helpers;

/**
 * Controller based on EPOCH and Mini-Batch of training data
 */
public class MiniBatchEpochController {

    /**
     * Current miniBatch that is being processed
     */
    protected short currentMiniBatch;

    /**
     * Current epoch that is being processed
     */
    protected short currentEpoch;

    /**
     * Total number of miniBatches to process
     */
    protected short miniBatches;

    /**
     * Total number of Epochs
     */
    protected short epochs;

    /**
     * Update the miniBatch Count and Epochs
     */
    public void setMiniBatchAndEpochs(short miniBatches, short epochs) {
        this.miniBatches = miniBatches;
        this.epochs = epochs;
    }

    /**
     * Finish a mini-batch and check if more are remaining
     */
    public boolean miniBatchFinishedCheckIfMore() {
        currentMiniBatch = (short) ((currentMiniBatch + 1) % miniBatches);
        if (currentMiniBatch == 0) currentEpoch++;
        return currentEpoch < epochs;
    }

    public void clear() {
        currentEpoch = 0;
        currentMiniBatch = 0;
    }

    /**
     * Returns the indices starting from 0 for this miniBatch iteration
     * [start_index, end_index)
     */
    public int[] getStartEndIndices(int datasetSize) {
        int miniBatchSize = (int) Math.ceil(((double) datasetSize / miniBatches));
        int startIndex = currentMiniBatch * miniBatchSize;
        int endIndex = Math.min(datasetSize, startIndex + miniBatchSize);
        return new int[]{startIndex, endIndex};
    }

}
