import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BagUnionMerger {

    public static class MergeResult {
        public final MergeMetrics metrics;
        public final List<String> lines; // "t:n" lines in memory
        public MergeResult(MergeMetrics m, List<String> l) { this.metrics = m; this.lines = l; }
    }

    /** Phase 2 (timed part): merge two sorted files into in-memory "t:n" lines. */
    public static MergeResult mergeToLines(Path sortedT1, Path sortedT2, IOTracker io) throws IOException {
        try (BufferedReader br1 = new BufferedReader(new FileReader(sortedT1.toFile()));
             BufferedReader br2 = new BufferedReader(new FileReader(sortedT2.toFile()))) {

            String l1 = br1.readLine(); if (l1 != null) io.noteReadLine();
            String l2 = br2.readLine(); if (l2 != null) io.noteReadLine();

            MergeMetrics metrics = new MergeMetrics();
            List<String> out = new ArrayList<>();

            while (l1 != null || l2 != null) {
                Record r1 = (l1 != null) ? new Record(l1) : null;
                Record r2 = (l2 != null) ? new Record(l2) : null;

                Record key;
                int count = 0;

                if (r2 == null || (r1 != null && r1.compareTo(r2) < 0)) {
                    key = r1;
                    count += 1;
                    // consume equals from br1
                    while (true) {
                        br1.mark(128);
                        String nx = br1.readLine();
                        if (nx != null) io.noteReadLine();
                        if (nx != null && new Record(nx).compareTo(key) == 0) {
                            count++;
                        } else {
                            if (nx != null) br1.reset();
                            l1 = br1.readLine();
                            // advance to next will be read next loop
                            if (l1 != null) io.noteReadLine();
                            break;
                        }
                    }
                } else if (r1 == null || r2.compareTo(r1) < 0) {
                    key = r2;
                    count += 1;
                    while (true) {
                        br2.mark(128);
                        String nx = br2.readLine();
                        if (nx != null) io.noteReadLine();
                        if (nx != null && new Record(nx).compareTo(key) == 0) {
                            count++;
                        } else {
                            if (nx != null) br2.reset();
                            l2 = br2.readLine();
                            if (l2 != null) io.noteReadLine();
                            break;
                        }
                    }
                } else {
                    // r1 == r2
                    key = r1;
                    // consume equals from br1
                    int c1 = 1;
                    while (true) {
                        br1.mark(128);
                        String nx = br1.readLine();
                        if (nx != null) io.noteReadLine();
                        if (nx != null && new Record(nx).compareTo(key) == 0) {
                            c1++; l1 = nx;
                        } else {
                            if (nx != null) br1.reset();
                            l1 = br1.readLine();
                            if (l1 != null) io.noteReadLine();
                            break;
                        }
                    }
                    // consume equals from br2
                    int c2 = 1;
                    while (true) {
                        br2.mark(128);
                        String nx = br2.readLine();
                        if (nx != null) io.noteReadLine();
                        if (nx != null && new Record(nx).compareTo(key) == 0) {
                            c2++;
                        } else {
                            if (nx != null) br2.reset();
                            l2 = br2.readLine();
                            if (l2 != null) io.noteReadLine();
                            break;
                        }
                    }
                    count = c1 + c2;
                }

                out.add(key.raw + ":" + count);
                metrics.distinctTuples++;
            }

            metrics.outputBlocks = MergeMetrics.blocksForTuples(metrics.distinctTuples);
            // Count the last partial read block for Phase 2
            io.flushPartialBlocks();
            return new MergeResult(metrics, out);
        }
    }


}
