import java.io.*;
import java.util.*;

/** Two-Phase Multiway Merge Sort  */
public class TPMMS {
    private static final int BLOCK_TUPLES = 40; // 4kb = 40 tuples
    private final int maxRecordsInMem;
    private final int fanIn;
    private final IOTracker io;                 // counts block I/Os

    public TPMMS(long memMB, IOTracker io) {
        this.io = io;
        long memBytes = memMB * 1024L * 1024L;
        long usable = (long) (memBytes * 0.6); // ~60% to be safe vs. object overhead
        this.maxRecordsInMem = (int) Math.max(1, usable / Record.TOTAL_WIDTH);
        this.fanIn = Math.max(2, (maxRecordsInMem / BLOCK_TUPLES) - 1);
    }
    public List<File> createInitialRuns(String filePath, String prefix) throws IOException {
        List<File> runs = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            List<Record> buffer = new ArrayList<>(maxRecordsInMem);
            String line;
            int runCount = 0;

            while ((line = br.readLine()) != null) {
                io.noteReadLine();
                buffer.add(new Record(line));
                if (buffer.size() >= maxRecordsInMem) {
                    runs.add(writeRun(buffer, prefix + "_run" + (++runCount) + ".txt"));
                    buffer.clear();
                }
            }
            // leftover
            if (!buffer.isEmpty()) {
                runs.add(writeRun(buffer, prefix + "_run" + (++runCount) + ".txt"));
            }
        }
        io.flushPartialBlocks();
        return runs;
    }

    public File mergeRuns(List<File> runs, String outputName) throws IOException {
        if (runs.isEmpty()) throw new IllegalArgumentException("No runs to merge");

        List<File> current = new ArrayList<>(runs);
        while (current.size() > 1) {
            List<File> next = new ArrayList<>();
            for (int i = 0; i < current.size(); i += fanIn) {
                int to = Math.min(i + fanIn, current.size());
                List<File> group = current.subList(i, to);
                File merged = mergeGroup(group);
                // delete inputs
                //for (File f : group) f.delete();
                next.add(merged);
            }
            current = next;
        }
        File finalRun = current.get(0);
        File output = new File(outputName);
        if (!finalRun.renameTo(output)) {
            // fallback copy
            try (BufferedReader br = new BufferedReader(new FileReader(finalRun));
                 BufferedWriter bw = new BufferedWriter(new FileWriter(output))) {
                String s;
                while ((s = br.readLine()) != null) {
                    io.noteReadLine();
                    bw.write(s);
                    bw.newLine();
                    io.noteWriteLine();
                }
            }
            //finalRun.delete();
            io.flushPartialBlocks();
            // finalize blocks for this copy pass
        }
        return output;
    }

    private File writeRun(List<Record> buffer, String runName) throws IOException {
        Collections.sort(buffer);
        File f = new File(runName);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
            for (Record r : buffer) {
                bw.write(r.raw);
                bw.newLine();
                io.noteWriteLine();
            }
        }
        // count the last partial write block for this run write
        io.flushPartialBlocks();
        return f;
    }

    private File mergeGroup(List<File> group) throws IOException {
        PriorityQueue<RunReader> pq = new PriorityQueue<>();
        for (File f : group) {
            RunReader rr = new RunReader(f);
            if (rr.current != null) pq.add(rr);
        }
        File out = File.createTempFile("tpmms_merge_", ".tmp");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(out))) {
            while (!pq.isEmpty()) {
                RunReader rr = pq.poll();
                bw.write(rr.current.raw);
                bw.newLine();
                io.noteWriteLine();

                if (rr.advance()) {
                    pq.add(rr);
                } else {
                    rr.close();
                }
            }
        }
        // partial write blocks for this merge
        io.flushPartialBlocks();
        return out;
    }

    private class RunReader implements Comparable<RunReader> {
        final BufferedReader br;
        Record current;

        RunReader(File f) throws IOException {
            this.br = new BufferedReader(new FileReader(f));
            advance(); // prime
        }

        boolean advance() throws IOException {
            String line = br.readLine();
            if (line == null) {
                current = null;
                return false;
            }
            io.noteReadLine();
            current = new Record(line);
            return true;
        }
        void close() throws IOException { br.close(); }
        @Override public int compareTo(RunReader o) { return current.compareTo(o.current); }
    }
}
