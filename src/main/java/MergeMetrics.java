public class MergeMetrics {
    public long distinctTuples;
    public long outputBlocks;
    // assuming 40 tuples per block

    public static long blocksForTuples(long tuples) {
        return (tuples + 39) / 40;
    }
}

