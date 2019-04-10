package store;

import java.io.*;
import java.util.Map;

public class Segment {
    int offset;

    File dataFile;
    RandomAccessFile reader;
    // This is just needed to be able to close the file, feels like there may
    // be a better way to do this
    FileOutputStream outputStream;
    DataOutputStream writer;

    // TODO: should be a distinction between segments that are readonly and segments that are being written to (the active segment)
    public Segment(File dataFile) throws IOException {
        this.reader = new RandomAccessFile(dataFile, "r");
        this.dataFile = dataFile;
        this.outputStream = new FileOutputStream(dataFile, true);
        this.writer = new DataOutputStream(outputStream);
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

    public IndexRecord put(String key, String value) throws IOException {
        writer.writeInt(key.length());
        writer.write(key.getBytes());
        int valueLength = value.getBytes().length;

        writer.writeInt(valueLength);
        writer.write(value.getBytes());
        offset += 8 + key.getBytes().length;

        IndexRecord record = new IndexRecord(dataFile.getName(),
                valueLength, offset);

        offset += valueLength;

        return record;
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

    public void close() throws Exception {
        reader.close();
        outputStream.close();
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






