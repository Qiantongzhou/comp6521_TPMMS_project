import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BagUnionMerger {

    public static MergeMetrics mergeAndWrite(Path sortedT1,
                                             Path sortedT2,
                                             IOTracker io,
                                             BufferedWriter out) throws IOException {
        MergeMetrics metrics = new MergeMetrics();

        try (BufferedReader br1 = new BufferedReader(new FileReader(sortedT1.toFile()));
             BufferedReader br2 = new BufferedReader(new FileReader(sortedT2.toFile()))) {

            String l1 = br1.readLine(); if (l1 != null) io.noteReadLine();
            String l2 = br2.readLine(); if (l2 != null) io.noteReadLine();

            while (l1 != null || l2 != null) {
                Record r1 = (l1 != null) ? new Record(l1) : null;
                Record r2 = (l2 != null) ? new Record(l2) : null;

                Record key;
                int count = 0;

                if (r2 == null || (r1 != null && r1.compareTo(r2) < 0)) {
                    // consume r1 only group...
                    key = r1;
                    count = 1;
                    l1 = br1.readLine();
                    if (l1 != null) io.noteReadLine();
                } else if (r1 == null || r2.compareTo(r1) < 0) {
                    key = r2;
                    count = 1;
                    l2 = br2.readLine();
                    if (l2 != null) io.noteReadLine();
                } else {
                    // r1 == r2 consume from both and sum multiplicities
                    key = r1;
                    int c1 = 1;
                    int c2 = 1;
                    l1 = br1.readLine();
                    if (l1 != null) io.noteReadLine();
                    l2 = br2.readLine();
                    if (l2 != null) io.noteReadLine();
                    count = c1 + c2;
                }

                // stream result out immediately
                out.write(key.raw);
                out.write(':');
                out.write(Integer.toString(count));
                out.newLine();
                io.noteWriteLine();

                metrics.distinctTuples++;
            }
        }

        metrics.outputBlocks = MergeMetrics.blocksForTuples(metrics.distinctTuples);
        io.flushPartialBlocks();
        return metrics;
    }
}

