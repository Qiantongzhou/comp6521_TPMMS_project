import java.io.*;
import java.nio.file.Path;
class MergeMetrics {
    public long distinctTuples;
    public long outputBlocks; // assuming 40 tuples per block

    public static long blocksForTuples(long tuples) {
        return (tuples + 39) / 40;
    }
}

public class BagUnionMerger {

    // merges two sorted files into t:n lines
    public static MergeMetrics merge(Path sortedT1, Path sortedT2, Writer out) throws IOException {
        try (BufferedReader br1 = new BufferedReader(new FileReader(sortedT1.toFile()));
             BufferedReader br2 = new BufferedReader(new FileReader(sortedT2.toFile()));
             BufferedWriter bw = (out instanceof BufferedWriter) ? (BufferedWriter) out : new BufferedWriter(out)) {

            String l1 = br1.readLine();
            String l2 = br2.readLine();

            MergeMetrics metrics = new MergeMetrics();

            while (l1 != null || l2 != null) {
                String next;
                int count = 0;

                if (l2 == null || (l1 != null && l1.compareTo(l2) < 0)) {
                    // all equal l1's
                    next = l1;
                    while (l1 != null && l1.equals(next)) { count++; l1 = br1.readLine(); }
                } else if (l1 == null || (l2 != null && l2.compareTo(l1) < 0)) {
                    // all equal l2's
                    next = l2;
                    while (l2 != null && l2.equals(next)) { count++; l2 = br2.readLine(); }
                } else {
                    // l1 == l2
                    next = l1;
                    while (l1 != null && l1.equals(next)) { count++; l1 = br1.readLine(); }
                    while (l2 != null && l2.equals(next)) { count++; l2 = br2.readLine(); }
                }

                bw.write(next);
                bw.write(':');
                bw.write(Integer.toString(count));
                bw.newLine();

                metrics.distinctTuples++;
                // one output line = one distinct tuple
            }

            bw.flush();
            metrics.outputBlocks = MergeMetrics.blocksForTuples(metrics.distinctTuples);
            return metrics;
        }
    }
}
