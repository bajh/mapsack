package store;

public class IndexRecord {
    String fileName;
    int valueLength;
    int valueOffset;

    IndexRecord(String fileName, int valueLength, int valueOffset) {
        this.fileName = fileName;
        this.valueLength = valueLength;
        this.valueOffset = valueOffset;
    }

    public synchronized void mergeWith(IndexRecord otherRecord) {
        this.fileName = otherRecord.fileName;
        this.valueOffset = otherRecord.valueOffset;
    }
}
