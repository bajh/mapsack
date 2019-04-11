package store;

import java.io.*;
import java.util.Map;

public class Segment {
    File dataFile;
    RandomAccessFile reader;

    public Segment(File dataFile) throws IOException {
        this.reader = new RandomAccessFile(dataFile, "r");
        this.dataFile = dataFile;
    }

    public void load(Map<String, IndexRecord> index) throws IOException {
        Segment segment = this;
        walk(new Segment.Visitor() {
            public void visit(String key, IndexRecord record) throws IOException {
                index.put(key, record);
            }
        });
    }

    public String get(IndexRecord record) throws IOException {
        reader.seek(record.valueOffset);
        byte[] value = new byte[record.valueLength];
        reader.readFully(value);
        return new String(value);
    }

    private void walk(Segment.Visitor visitor) throws IOException {
        int keyLength;
        int valueLength;
        int offset = 0;

        try (FileInputStream fis = new FileInputStream(dataFile)) {
            DataInputStream din = new DataInputStream(fis);

            while (true) {
                try {
                    keyLength = din.readInt();
                } catch (EOFException _) {
                    return;
                }

                byte[] keyBuf = new byte[keyLength];
                din.readFully(keyBuf);
                valueLength = din.readInt();
                offset += 8 + keyLength;

                visitor.visit(new String(keyBuf),
                    new IndexRecord(dataFile.getName(), valueLength, offset));

                din.skipBytes(valueLength);
                offset += valueLength;
            }

        }
    }

    public String getFileName() {
        return dataFile.getName();
    }

    public void close() throws Exception {
        reader.close();
    }

    public static interface Visitor {
        public void visit(String key, IndexRecord record) throws IOException;
    }

    public static class CorruptedSegmentFileException extends RuntimeException {
        public CorruptedSegmentFileException(String errorMessage) {
            super(errorMessage);
        }
    }
}
