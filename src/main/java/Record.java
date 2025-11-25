
public class Record implements Comparable<Record> {

    // Field widths sum must be 100
    private static final int W_ID = 8;       // Student ID (int, 08)
    private static final int W_FIRST = 10;   // First Name (char, 10)
    private static final int W_LAST = 10;    // Last Name  (char, 10)
    private static final int W_DEPT = 3;     // Department (int, 03)
    private static final int W_PROG = 3;     // Program    (int, 03)
    private static final int W_SIN = 9;      // SIN        (int, 09)
    private static final int W_ADDR = 56;    // Address    (char, 56)
    public static final int TOTAL_WIDTH = W_ID + W_FIRST + W_LAST + W_DEPT + W_PROG + W_SIN + W_ADDR; // 100
    private final String studentId; // length 8
    private final String firstName; // length 10
    private final String lastName;  // length 10
    private final String department;// length 3
    private final String program;   // length 3
    private final String sin;       // length 9
    private final String address;   // length 56
    public final String raw;

    public Record(String line) {
        if (line == null) throw new IllegalArgumentException("line is null");
        if (!line.isEmpty() && line.charAt(line.length() - 1) == '\r') line = line.substring(0, line.length() - 1);

        if (line.length() < TOTAL_WIDTH) {
            throw new IllegalArgumentException("Record line shorter than " + TOTAL_WIDTH + " chars: " + line.length());
        }
        String fixed = line.substring(0, TOTAL_WIDTH);
        int p = 0;
        String id    = fixed.substring(p, p += W_ID);
        String first = fixed.substring(p, p += W_FIRST);
        String last  = fixed.substring(p, p += W_LAST);
        String dept  = fixed.substring(p, p += W_DEPT);
        String prog  = fixed.substring(p, p += W_PROG);
        String sin9  = fixed.substring(p, p += W_SIN);
        String addr  = fixed.substring(p, p += W_ADDR);

        this.studentId = padLeftDigits(stripSpaces(id), W_ID);
        this.firstName = padRightSpaces(rtrim(first), W_FIRST);
        this.lastName  = padRightSpaces(rtrim(last), W_LAST);
        this.department= padLeftDigits(stripSpaces(dept), W_DEPT);
        this.program   = padLeftDigits(stripSpaces(prog), W_PROG);
        this.sin       = padLeftDigits(stripSpaces(sin9), W_SIN);
        this.address   = padRightSpaces(rtrim(addr), W_ADDR);
        this.raw = serialize();
    }


    private String serialize() {
        StringBuilder sb = new StringBuilder(TOTAL_WIDTH);
        sb.append(studentId)
                .append(firstName)
                .append(lastName)
                .append(department)
                .append(program)
                .append(sin)
                .append(address);
        if (sb.length() != TOTAL_WIDTH) {
            throw new IllegalStateException("Serialized length != " + TOTAL_WIDTH + ": " + sb.length());
        }
        return sb.toString();
    }
    @Override public int compareTo(Record other) { return this.raw.compareTo(other.raw); }

    @Override public String toString() { return raw; }
    @Override public int hashCode() { return raw.hashCode(); }
    @Override public boolean equals(Object o) { return (o instanceof Record) && raw.equals(((Record)o).raw); }

    private static String rtrim(String s) {
        int i = s.length();
        while (i > 0 && s.charAt(i - 1) == ' ') i--;
        return (i == s.length()) ? s : s.substring(0, i);
    }
    private static String stripSpaces(String s) {
        return s.replace(" ", "");
    }
    private static String padRightSpaces(String s, int width) {
        if (s.length() > width) return s.substring(0, width);
        StringBuilder sb = new StringBuilder(width);
        sb.append(s);
        while (sb.length() < width) sb.append(' ');
        return sb.toString();
    }
    private static String padLeftDigits(String s, int width) {
        String digits = s;
        if (digits.length() > width) return digits.substring(digits.length() - width); // keep rightmost width chars
        StringBuilder sb = new StringBuilder(width);
        for (int i = digits.length(); i < width; i++) sb.append('0');
        sb.append(digits);
        return sb.toString();
    }
}
