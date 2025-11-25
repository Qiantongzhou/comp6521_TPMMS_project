import java.io.*;
import java.util.*;

/** Two-Phase Multiway Merge Sort  */
public class TPMMS {
    private static final int BLOCK_TUPLES = 40; // 4kb = 40 tuples
    private final int maxRecordsInMem;
    private final int K;
    private final IOTracker io;
    // counts block I/Os

    public TPMMS(long memMB, IOTracker io) {
        this.io = io;
        long memBytes = memMB * 1024L * 1024L;
        long usable = (long) (memBytes * 0.6); // ~60% to be safe
        this.maxRecordsInMem = (int) Math.max(1, usable / Record.TOTAL_WIDTH);
        this.K = Math.max(2, (maxRecordsInMem / BLOCK_TUPLES) - 1);
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


    public File multiPassMerge(List<File> initialRuns, String relName) throws IOException {
        if (initialRuns == null || initialRuns.isEmpty()) {
            return null;
        }

        List<File> currentRuns = new ArrayList<>(initialRuns);
        int pass = 0;

        System.out.println("=== " + relName + " Phase 2: external merge sort (pairwise) ===");
        System.out.println("Initial runs r = " + currentRuns.size());

        while (currentRuns.size() > 1) {
            pass++;
            System.out.println("\n-- " + relName + " Pass " + pass + " --");
            System.out.println("  Input runs this pass: " + currentRuns.size());

            List<File> nextRuns = new ArrayList<>();

            // Merge runs in PAIRS
            for (int i = 0; i < currentRuns.size(); i += 2) {
                if (i + 1 < currentRuns.size()) {
                    File left  = currentRuns.get(i);
                    File right = currentRuns.get(i + 1);

                    System.out.println("    Merging runs " + i + " and " + (i + 1)
                            + " (" + left.getName() + ", " + right.getName() + ")");

                    File merged = mergeTwoRuns(left, right);
                    nextRuns.add(merged);


                } else {
                    System.out.println("    Carrying over run " + i + " (" + currentRuns.get(i).getName() + ")");
                    nextRuns.add(currentRuns.get(i));
                }
            }

            currentRuns = nextRuns;
            System.out.println("  After pass " + pass + " we have " + currentRuns.size() + " runs.");
        }

        File finalRun = currentRuns.get(0);
        System.out.println("\n=== " + relName + " Phase 2 merge sort done in " + pass
                + " passes; final sorted file: " + finalRun.getName() + " ===");

        return finalRun;
    }

    private File mergeTwoRuns(File leftFile, File rightFile) throws IOException {
        File out = File.createTempFile("tpmms_mergesort_", ".tmp");

        try (BufferedReader br1 = new BufferedReader(new FileReader(leftFile));
             BufferedReader br2 = new BufferedReader(new FileReader(rightFile));
             BufferedWriter bw = new BufferedWriter(new FileWriter(out))) {

            String l1 = br1.readLine();
            if (l1 != null) io.noteReadLine();
            String l2 = br2.readLine();
            if (l2 != null) io.noteReadLine();

            Record r1 = (l1 != null) ? new Record(l1) : null;
            Record r2 = (l2 != null) ? new Record(l2) : null;

            //  2 way merge of sorted sequences
            while (r1 != null && r2 != null) {
                if (r1.compareTo(r2) <= 0) {
                    bw.write(r1.raw);
                    bw.newLine();
                    io.noteWriteLine();

                    l1 = br1.readLine();
                    if (l1 != null) io.noteReadLine();
                    r1 = (l1 != null) ? new Record(l1) : null;
                } else {
                    bw.write(r2.raw);
                    bw.newLine();
                    io.noteWriteLine();

                    l2 = br2.readLine();
                    if (l2 != null) io.noteReadLine();
                    r2 = (l2 != null) ? new Record(l2) : null;
                }
            }

            // Drain leftovers from whichever run still has tuples
            while (r1 != null) {
                bw.write(r1.raw);
                bw.newLine();
                io.noteWriteLine();

                l1 = br1.readLine();
                if (l1 != null) io.noteReadLine();
                r1 = (l1 != null) ? new Record(l1) : null;
            }

            while (r2 != null) {
                bw.write(r2.raw);
                bw.newLine();
                io.noteWriteLine();

                l2 = br2.readLine();
                if (l2 != null) io.noteReadLine();
                r2 = (l2 != null) ? new Record(l2) : null;
            }
        }

        // Count partial blocks for this merge
        io.flushPartialBlocks();

        return out;
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

}
