public record IntRecord(int value, int file_index) implements Comparable<IntRecord> {

    public int getValue() {return value;}

    public int getFileIndex() {return file_index;}

    @Override public int compareTo(IntRecord o) {
        return Integer.compare(this.value, o.value);
    }
}