public class IOTracker {
    private static final int BLOCK_TUPLES = 40;
    private long readLines = 0, writtenLines = 0;
    public long blocksRead = 0, blocksWritten = 0;

    public void noteReadLine() {
        if (++readLines % BLOCK_TUPLES == 0) blocksRead++;
    }
    public void noteWriteLine() {
        if (++writtenLines % BLOCK_TUPLES == 0) blocksWritten++;
    }
    public void flushPartialBlocks() {
        if (readLines % BLOCK_TUPLES != 0) blocksRead++;
        if (writtenLines % BLOCK_TUPLES != 0) blocksWritten++;
        readLines = 0; writtenLines = 0; // reset per phase if you want per-phase numbers
    }
}
