import java.io.*;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
        String t1Path = (args.length > 1) ? args[1] : "src/inputfile/T1_records_1m.txt";
        String t2Path = (args.length > 2) ? args[2] : "src/inputfile/T2_records_1m.txt";
        clearOutputDir("src/outputfile");
        clearOutputDir("src/outputfile/runs");
        long maxHeapMB = Runtime.getRuntime().maxMemory() / (1024 * 1024);

        long memMB=maxHeapMB/5;
        System.out.println("JVM maxHeap (MB) = " + maxHeapMB + " | memMB for TPMMS = " + memMB);
        if (maxHeapMB < memMB) {
            System.out.println("WARNING: -Xmx is less than requested memMB; set e.g. -Xmx" + (memMB + 64) + "m");
        }
        IOTracker io = new IOTracker();
        TPMMS sorter = new TPMMS(memMB, io);

        long p1t1Start = System.currentTimeMillis();
        List<File> t1Runs = sorter.createInitialRuns(t1Path, "src/outputfile/runs/T1");
        File sortedT1 = sorter.mergeRuns(t1Runs, "src/outputfile/T1_sorted.txt");
        long p1t1End = System.currentTimeMillis();
        System.out.println("Phase 1 (T1 sort): " + (p1t1End - p1t1Start) + " ms, I/Os so far R=" + io.totalBlocksRead + " W=" + io.totalBlocksWritten);

        long p1t2Start = System.currentTimeMillis();
        List<File> t2Runs = sorter.createInitialRuns(t2Path, "src/outputfile/runs/T2");
        File sortedT2 = sorter.mergeRuns(t2Runs, "src/outputfile/T2_sorted.txt");
        long p1t2End = System.currentTimeMillis();
        System.out.println("Phase 1 (T2 sort): " + (p1t2End - p1t2Start) + " ms, I/Os cumulative R=" + io.totalBlocksRead + " W=" + io.totalBlocksWritten);

        long p2Start = System.currentTimeMillis();

        File output = new File("src/outputfile/BagUnion_Output.txt");
        MergeMetrics resultMetrics;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(output))) {
            resultMetrics = BagUnionMerger.mergeAndWrite(
                    sortedT1.toPath(),
                    sortedT2.toPath(),
                    io,
                    bw);
        }

        System.out.println("Distinct tuples: " + resultMetrics.distinctTuples);
        System.out.println("Output blocks (40 tuples/block): " + resultMetrics.outputBlocks);
        System.out.println("Total I/Os after write R=" + io.totalBlocksRead + " W=" + io.totalBlocksWritten);
        long p2End = System.currentTimeMillis();
        System.out.println("Phase 2 (bag-union, in-memory): " + (p2End - p2Start) + " ms");

    }
    private static void clearOutputDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            // create it if it doesn't exist
            if (!dir.mkdirs()) {
                System.err.println("Could not create output directory: " + dirPath);
            }
            return;
        }

        File[] files = dir.listFiles();
        if (files == null) return;

        for (File f : files) {
            if (f.isDirectory()) {
                // if you never put subdirs here, you can skip this block
                File[] inner = f.listFiles();
                if (inner != null) {
                    for (File c : inner) {
                        if (!c.delete()) {
                            System.err.println("Could not delete: " + c.getAbsolutePath());
                        }
                    }
                }
            }
            if (!f.delete()) {
                System.err.println("Could not delete: " + f.getAbsolutePath());
            }
        }
    }

}
