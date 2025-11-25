public class IOTracker {

    // 1 disk block holds 40 records
    private static final int TUPLES_PER_BLOCK = 40;

    private int readTuplesInCurrentBlock = 0;
    private int writtenTuplesInCurrentBlock = 0;

    public long totalBlocksRead = 0;
    public long totalBlocksWritten = 0;

    public void noteReadLine() {
        readTuplesInCurrentBlock++;

        if (readTuplesInCurrentBlock == TUPLES_PER_BLOCK) {
            totalBlocksRead++;
            readTuplesInCurrentBlock = 0;
            // start counting the next block
        }
    }

    public void noteWriteLine() {
        writtenTuplesInCurrentBlock++;

        if (writtenTuplesInCurrentBlock == TUPLES_PER_BLOCK) {
            totalBlocksWritten++;
            writtenTuplesInCurrentBlock = 0;
        }
    }


    public void flushPartialBlocks() {
        if (readTuplesInCurrentBlock > 0) {
            totalBlocksRead++;
        }
        if (writtenTuplesInCurrentBlock > 0) {
            totalBlocksWritten++;
        }

        readTuplesInCurrentBlock = 0;
        writtenTuplesInCurrentBlock = 0;
    }
}
