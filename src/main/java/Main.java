import java.io.*;
import java.util.*;


public class Main {

    public static void main(String[] args) throws Exception {
        String t1Path = "src/inputfile/T1_records.txt";
        String t2Path = "src/inputfile/T2_records.txt";
        long maxHeap = Runtime.getRuntime().maxMemory();
        System.out.println("maxHeap = " + maxHeap);
        System.out.println("maxheap size (mb) = " + maxHeap/(1024*1024));
        if(maxHeap/(1024*1024)>200){
            System.out.println("===incorrect maxheap size, need config===");
        }
        // Phase 1  Sort both tables individually using TPMMS
        TPMMS sorter = new TPMMS();
        long start1 = System.currentTimeMillis();
        List<File> t1SortedRuns = sorter.createInitialRuns(t1Path, "src/outputfile/T1");
        File sortedT1 = sorter.mergeRuns(t1SortedRuns, "src/outputfile/T1_sorted.txt");
        long end1 = System.currentTimeMillis();
        System.out.println("Phase 1 (T1): " + (end1 - start1) + " ms");

        long start2 = System.currentTimeMillis();
        List<File> t2SortedRuns = sorter.createInitialRuns(t2Path, "src/outputfile/T2");
        File sortedT2 = sorter.mergeRuns(t2SortedRuns, "src/outputfile/T2_sorted.txt");
        long end2 = System.currentTimeMillis();
        System.out.println("Phase 1 (T2): " + (end2 - start2) + " ms");

        // Phase 2  Merge to produce bag union
        long startMerge = System.currentTimeMillis();
        File output = new File("src/outputfile/BagUnion_Output.txt");
        MergeMetrics mm = BagUnionMerger.merge(sortedT1.toPath(), sortedT2.toPath(), new FileWriter(output));
        System.out.println("Distinct tuples: " + mm.distinctTuples);
        System.out.println("Output blocks (40 tuples/block): " + mm.outputBlocks);
        long endMerge = System.currentTimeMillis();
        System.out.println("Phase 2 (Bag Union): " + (endMerge - startMerge) + " ms");
    }
}
