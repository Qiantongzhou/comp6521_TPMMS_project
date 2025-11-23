package util;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class RandomStudentDataGenerator {

    // Default number of records (1 million)
    private static final int DEFAULT_RECORD_COUNT = 1_000_000;

    // Field lengths from the project description
    // 1. Student ID: int(08)
    // 2. First Name: char(10)
    // 3. Last Name: char(10)
    // 4. Department: int(03)
    // 5. Program: int(03)
    // 6. SIN Number: int(09)
    // 7. Address: char(56)
    private static final int ID_LEN = 8;
    private static final int FIRST_NAME_LEN = 10;
    private static final int LAST_NAME_LEN = 10;
    private static final int DEPT_LEN = 3;
    private static final int PROGRAM_LEN = 3;
    private static final int SIN_LEN = 9;
    private static final int ADDRESS_LEN = 56;

    // Some sample names and street names to keep things readable
    private static final String[] FIRST_NAMES = {
            "John", "Mary", "Alice", "Bob", "David",
            "Sarah", "Kevin", "Laura", "Emily", "Michael"
    };

    private static final String[] LAST_NAMES = {
            "Smith", "Johnson", "Brown", "Lee", "Martin",
            "Garcia", "Lopez", "Wilson", "Taylor", "Clark"
    };

    private static final String[] STREET_NAMES = {
            "Maisonneuve West", "Sherbrooke Street", "Saint Catherine",
            "Guy Street", "Peel Street", "Saint Denis",
            "Saint Laurent", "Crescent Street", "Park Avenue", "Bishop Street"
    };

    private static final String[] STREET_TYPES = {
            "St", "Ave", "Blvd", "Rd"
    };

    public static void main(String[] args) {
        String outputFile = (args.length > 0) ? args[0] : "src/inputfile/T2_records_1m.txt";
        int recordCount = (args.length > 1) ? Integer.parseInt(args[1]) : DEFAULT_RECORD_COUNT;

        // Use ASCII so that 1 char = 1 byte, matching the "100 bytes per record" assumption.
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.US_ASCII))) {

            Random random = new Random(); // you can use a fixed seed for reproducibility if you want

            for (int i = 0; i < recordCount; i++) {
                String record = generateRecord(random);
                writer.write(record);
                writer.newLine(); // each tuple on its own line (Unix-style file)
            }

            System.out.println("Generated " + recordCount + " records into " + outputFile);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String generateRecord(Random random) {
        StringBuilder sb = new StringBuilder();

        // 1. Student ID: 8 digits, no leading zero
        String studentId = randomNumericString(random, ID_LEN, false);
        sb.append(studentId);

        // 2. First Name: char(10), padded / truncated
        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        sb.append(padOrTruncate(firstName, FIRST_NAME_LEN));

        // 3. Last Name: char(10), padded / truncated
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
        sb.append(padOrTruncate(lastName, LAST_NAME_LEN));

        // 4. Department: int(03), e.g. 444, 555, 666, 777
        int[] departments = {444, 555, 666, 777};
        int dept = departments[random.nextInt(departments.length)];
        sb.append(padLeftWithZeros(Integer.toString(dept), DEPT_LEN));

        // 5. Program: int(03), some random code 100–999
        int program = 100 + random.nextInt(900);
        sb.append(padLeftWithZeros(Integer.toString(program), PROGRAM_LEN));

        // 6. SIN Number: 9 digits, no leading zero
        String sin = randomNumericString(random, SIN_LEN, false);
        sb.append(sin);

        // 7. Address: char(56), padded / truncated
        String address = randomAddress(random);
        sb.append(padOrTruncate(address, ADDRESS_LEN));

        // At this point, sb has exactly 8+10+10+3+3+9+56 = 99 characters.
        // The assignment says 100 bytes per record; that mismatch is likely off by 1 in the spec.
        // If you *must* hit exactly 100, you could make ADDRESS_LEN = 57 instead.

        return sb.toString();
    }

    private static String randomAddress(Random random) {
        int houseNo = 100 + random.nextInt(9900); // 100–9999
        String streetName = STREET_NAMES[random.nextInt(STREET_NAMES.length)];
        String streetType = STREET_TYPES[random.nextInt(STREET_TYPES.length)];

        // Example: "1455 Maisonneuve West, Montreal, QC, H3G 1M8"
        String base = houseNo + " " + streetName + " " + streetType
                + ", Montreal, QC, H3G 1M8";
        return base;
    }

    private static String padOrTruncate(String value, int length) {
        if (value.length() == length) {
            return value;
        } else if (value.length() > length) {
            return value.substring(0, length);
        } else {
            StringBuilder sb = new StringBuilder(length);
            sb.append(value);
            while (sb.length() < length) {
                sb.append(' ');
            }
            return sb.toString();
        }
    }

    private static String padLeftWithZeros(String value, int length) {
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(length);
        while (sb.length() + value.length() < length) {
            sb.append('0');
        }
        sb.append(value);
        return sb.toString();
    }

    /**
     * Generates a numeric string of a given number of digits.
     *
     * @param random       Random instance
     * @param digits       number of digits
     * @param allowLeadingZero true if first digit can be 0
     */
    private static String randomNumericString(Random random, int digits, boolean allowLeadingZero) {
        StringBuilder sb = new StringBuilder(digits);
        for (int i = 0; i < digits; i++) {
            int digit;
            if (i == 0 && !allowLeadingZero) {
                digit = 1 + random.nextInt(9); // 1–9
            } else {
                digit = random.nextInt(10);   // 0–9
            }
            sb.append(digit);
        }
        return sb.toString();
    }
}
