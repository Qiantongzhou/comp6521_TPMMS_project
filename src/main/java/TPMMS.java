import java.io.*;
import java.util.*;

public class TPMMS {
    private static final int BLOCK_SIZE = 40; // tuples per block
    private static final int MEMORY_LIMIT = 40 * 50; // adjust per Xmx

    // Phase 1 Create sorted runs
    public List<File> createInitialRuns(String filePath, String prefix) throws IOException {
        List<File> runs = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        List<Record> buffer = new ArrayList<>(MEMORY_LIMIT);
        String line;
        int runCount = 0;

        while ((line = br.readLine()) != null) {
            buffer.add(new Record(line));
            if (buffer.size() >= MEMORY_LIMIT) {
                runCount++;
                runs.add(writeSortedRun(buffer, prefix + "_run" + runCount + ".txt"));
                buffer.clear();
            }
        }
        if (!buffer.isEmpty()) {
            runCount++;
            runs.add(writeSortedRun(buffer, prefix + "_run" + runCount + ".txt"));
        }
        br.close();
        return runs;
    }

    private File writeSortedRun(List<Record> buffer, String filename) throws IOException {
        Collections.sort(buffer);
        File runFile = new File(filename);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(runFile))) {
            for (Record r : buffer) {
                bw.write(r.raw);
                bw.newLine();
            }
        }
        return runFile;
    }

    // Phase 1 cont Multiway merge runs
    public File mergeRuns(List<File> runs, String outputName) throws IOException {
        PriorityQueue<RunReader> pq = new PriorityQueue<>();
        for (File f : runs) pq.add(new RunReader(f));

        File output = new File(outputName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));

        while (!pq.isEmpty()) {
            RunReader rr = pq.poll();
            bw.write(rr.current.raw);
            bw.newLine();

            if (rr.advance()) pq.add(rr);
            else rr.close();
        }

        bw.close();
        return output;
    }

    private static class RunReader implements Comparable<RunReader> {
        BufferedReader br;
        Record current;

        RunReader(File file) throws IOException {
            br = new BufferedReader(new FileReader(file));
            advance();
        }

        boolean advance() throws IOException {
            String line = br.readLine();
            if (line == null) return false;
            current = new Record(line);
            return true;
        }

        void close() throws IOException { br.close(); }

        @Override
        public int compareTo(RunReader other) {
            return current.compareTo(other.current);
        }
    }
}
