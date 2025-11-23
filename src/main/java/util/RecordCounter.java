package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class RecordCounter {

    // Number of tuples per block in your project
    private static final int TUPLES_PER_BLOCK = 40;

    public static long countRecords(Path file) throws IOException {
        long count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
            while (br.readLine() != null) {
                count++;
            }
        }
        return count;
    }

    public static void main(String[] args) {


        try {
            Path file = Path.of("src/inputfile/T1_records_1m.txt");
            long records = countRecords(file);
            long blocks = (records + TUPLES_PER_BLOCK - 1) / TUPLES_PER_BLOCK; // ceil

            System.out.println("File: " + file);
            System.out.println("Total records : " + records);
            System.out.println("Blocks (40 tuples/block): " + blocks);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            System.exit(2);
        }
    }
}
