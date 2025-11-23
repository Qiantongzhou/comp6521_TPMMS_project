public class IOTracker {

    //1 disk block holds 40 records
    private static final int TUPLES_PER_BLOCK = 40;

    private long linesReadSinceReset = 0;
    private long linesWrittenSinceReset = 0;

    public long totalBlocksRead = 0;
    public long totalBlocksWritten = 0;

    /**
     * Call this method every time  read ONE line from a file.
     */
    public void noteReadLine() {
        linesReadSinceReset++;


        if (linesReadSinceReset % TUPLES_PER_BLOCK == 0) {
            totalBlocksRead++;
        }
    }

    /**
     * Call this method every time  write ONE line to a file.
     */
    public void noteWriteLine() {
        linesWrittenSinceReset++;

        if (linesWrittenSinceReset % TUPLES_PER_BLOCK == 0) {
            totalBlocksWritten++;
        }
    }

    /**
     * This method is called at the end of operation.
     * It accounts for any partial blocks that were used.
     */
    public void flushPartialBlocks() {

        if (linesReadSinceReset % TUPLES_PER_BLOCK != 0) {
            totalBlocksRead++;
        }

        if (linesWrittenSinceReset % TUPLES_PER_BLOCK != 0) {
            totalBlocksWritten++;
        }

        linesReadSinceReset = 0;
        linesWrittenSinceReset = 0;
    }
}

