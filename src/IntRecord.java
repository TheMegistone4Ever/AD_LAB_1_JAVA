public class IntRecord implements Comparable<IntRecord> {
    private final int value;
    private final int file_index;

    public IntRecord(int value, int file_index) {
        this.value = value;
        this.file_index = file_index;
    }

    public int getValue() {return value;}

    public int getFileIndex() {return file_index;}

    @Override
    public int compareTo(IntRecord o) {
        return Integer.compare(this.value, o.value);
    }
}