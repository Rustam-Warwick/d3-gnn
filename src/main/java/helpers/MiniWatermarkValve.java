package helpers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

public class MiniWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & watermark status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & watermark statuses are
     * fed into the valve.
     */
    private final InputChannelStatus[] channelStatuses;

    /**
     * The last watermark emitted from the valve.
     */
    private long lastOutputWatermark;

    /**
     * The last watermark status emitted from the valve.
     */
    private final WatermarkStatus lastOutputWatermarkStatus;

    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public MiniWatermarkValve(int numInputChannels) {
        checkArgument(numInputChannels > 0);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = Long.MIN_VALUE;
            channelStatuses[i].watermarkStatus = WatermarkStatus.ACTIVE;
            channelStatuses[i].isWatermarkAligned = true;
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputWatermarkStatus = WatermarkStatus.ACTIVE;
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link PushingAsyncDataInput.DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *
     * @param watermark    the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *                     starting from 0)
     */
    public Watermark inputWatermark(Watermark watermark, int channelIndex)
            throws Exception {
        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (lastOutputWatermarkStatus.isActive()
                && channelStatuses[channelIndex].watermarkStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its input
            // channel, ignore it also.
            if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = watermarkMillis;

                // previously unaligned input channels are now aligned if its watermark has caught
                // up
                if (!channelStatuses[channelIndex].isWatermarkAligned
                        && watermarkMillis >= lastOutputWatermark) {
                    channelStatuses[channelIndex].isWatermarkAligned = true;
                }

                // now, attempt to find a new min watermark across all aligned channels
                return findAndReturnNewMinWatermarkAcrossAllChannels();
            }
        }
        return null;
    }

    private Watermark findAndReturnNewMinWatermarkAcrossAllChannels()
            throws Exception {
        long newMinWatermark = Long.MAX_VALUE;
        boolean hasAlignedChannels = false;

        // determine new overall watermark by considering only watermark-aligned channels across all
        // channels
        for (InputChannelStatus channelStatus : channelStatuses) {
            if (channelStatus.isWatermarkAligned) {
                hasAlignedChannels = true;
                newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
            }
        }

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned channel, and is also larger than the last output watermark
        if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
            lastOutputWatermark = newMinWatermark;
            new Watermark(lastOutputWatermark);
        }
        return null;
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current watermark status of the channel is idle
     *   <li>the watermark status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     */
    @VisibleForTesting
    protected static class InputChannelStatus {
        protected long watermark;
        protected WatermarkStatus watermarkStatus;
        protected boolean isWatermarkAligned;

        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         */
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.watermarkStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }
    }
}

