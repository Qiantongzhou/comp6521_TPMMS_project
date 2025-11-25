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

        // PHASE 1: create runs for T1 and T2
        long p1Start = System.currentTimeMillis();

        List<File> t1Runs = sorter.createInitialRuns(t1Path, "src/outputfile/runs/T1");
        List<File> t2Runs = sorter.createInitialRuns(t2Path, "src/outputfile/runs/T2");

        long p1End = System.currentTimeMillis();
        System.out.println("Phase 1 (run generation only): " + (p1End - p1Start) + " ms");
        System.out.println("After Phase 1, total I/Os R=" + io.totalBlocksRead + " W=" + io.totalBlocksWritten);
        System.out.println("T1 runs: " + t1Runs.size() + ", T2 runs: " + t2Runs.size());
  // PHASE 2: merge all runs for T1 and T2, then bag union

        long p2Start = System.currentTimeMillis();

        //  TPMMS for T1 and T2
        File t1Sorted = sorter.multiPassMerge(t1Runs, "T1");
        File t2Sorted = sorter.multiPassMerge(t2Runs, "T2");

        File output = new File("src/outputfile/BagUnion_Output.txt");
        MergeMetrics resultMetrics;
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(output))) {
            resultMetrics = BagUnionMerger.mergeAndWrite(
                    t1Sorted.toPath(),
                    t2Sorted.toPath(),
                    io,
                    bw);
        }

        long p2End = System.currentTimeMillis();

        System.out.println("Distinct tuples: " + resultMetrics.distinctTuples);
        System.out.println("Output blocks (40 tuples/block): " + resultMetrics.outputBlocks);
        System.out.println("After Phase 2, total I/Os R=" + io.totalBlocksRead + " W=" + io.totalBlocksWritten);
        System.out.println("Phase 2 (TPMMS multi-pass + bag union): " + (p2End - p2Start) + " ms");

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
