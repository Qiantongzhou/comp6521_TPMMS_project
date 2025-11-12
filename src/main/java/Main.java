import java.io.*;
import java.util.*;

public class Main {

    // Usage: java -Xmx128m Main [memMB] [T1 path] [T2 path]
    public static void main(String[] args) throws Exception {
        String t1Path = (args.length > 1) ? args[1] : "src/inputfile/T1_records.txt";
        String t2Path = (args.length > 2) ? args[2] : "src/inputfile/T2_records.txt";
        long memMB = (args.length > 0) ? Long.parseLong(args[0]) : 51;

        long maxHeapMB = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        System.out.println("JVM maxHeap (MB) = " + maxHeapMB + " | memMB for TPMMS = " + maxHeapMB);
        if (maxHeapMB < memMB) {
            System.out.println("WARNING: -Xmx is less than requested memMB; set e.g. -Xmx" + (memMB + 64) + "m");
        }
        memMB=maxHeapMB;
        // Shared I/O tracker (clear between phases if you want per-phase numbers)
        IOTracker io = new IOTracker();
        TPMMS sorter = new TPMMS(memMB, io);

        // === Phase 1: sort T1 ===
        long p1t1Start = System.currentTimeMillis();
        List<File> t1Runs = sorter.createInitialRuns(t1Path, "src/outputfile/T1");
        File sortedT1 = sorter.mergeRuns(t1Runs, "src/outputfile/T1_sorted.txt");
        long p1t1End = System.currentTimeMillis();
        System.out.println("Phase 1 (T1 sort): " + (p1t1End - p1t1Start) + " ms, I/Os so far R=" + io.blocksRead + " W=" + io.blocksWritten);

        // === Phase 1: sort T2 ===
        long p1t2Start = System.currentTimeMillis();
        List<File> t2Runs = sorter.createInitialRuns(t2Path, "src/outputfile/T2");
        File sortedT2 = sorter.mergeRuns(t2Runs, "src/outputfile/T2_sorted.txt");
        long p1t2End = System.currentTimeMillis();
        System.out.println("Phase 1 (T2 sort): " + (p1t2End - p1t2Start) + " ms, I/Os cumulative R=" + io.blocksRead + " W=" + io.blocksWritten);

        // === Phase 2: merge to memory (TIMED), then write output (NOT timed) ===
        long p2Start = System.currentTimeMillis();
        BagUnionMerger.MergeResult mr = BagUnionMerger.mergeToLines(sortedT1.toPath(), sortedT2.toPath(), io);
        long p2End = System.currentTimeMillis();
        System.out.println("Phase 2 (bag-union, in-memory): " + (p2End - p2Start) + " ms");

        // Write result to disk (NOT included in Phase 2 time)
        File output = new File("src/outputfile/BagUnion_Output.txt");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(output))) {
            for (String line : mr.lines) {
                bw.write(line);
                bw.newLine();
                io.noteWriteLine();
            }
        }
        io.flushPartialBlocks();

        System.out.println("Distinct tuples: " + mr.metrics.distinctTuples);
        System.out.println("Output blocks (40 tuples/block): " + mr.metrics.outputBlocks);
        System.out.println("Total I/Os after write R=" + io.blocksRead + " W=" + io.blocksWritten);
    }
}
