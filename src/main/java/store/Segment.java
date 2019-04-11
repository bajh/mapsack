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
                if (record == null) {
                    index.remove(key);
                    return;
                }
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

    boolean isTombstoneBitSet(byte recordMeta) {
        return (recordMeta & 0x01) > 0;
    }

    public void walk(Segment.Visitor visitor) throws IOException {
        byte meta;
        int keyLength;
        int valueLength;
        int offset = 0;

        try (FileInputStream fis = new FileInputStream(dataFile)) {
            DataInputStream din = new DataInputStream(fis);

            while (true) {
                try {
                    meta = din.readByte();
                } catch (EOFException _) {
                    return;
                }

                keyLength = din.readInt();

                byte[] keyBuf = new byte[keyLength];
                din.readFully(keyBuf);
                offset += 5 + keyLength;
                if (isTombstoneBitSet(meta)) {
                    visitor.visit(new String(keyBuf), null);
                    return;
                }

                valueLength = din.readInt();
                offset += 4;

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
