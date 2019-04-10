package store;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    Map<String, IndexRecord> index;
    String activeFile;
    RandomAccessFile in;
    FileOutputStream outFile;
    DataOutput out;
    int offset;

    public HashIndexStore(File activeFile) throws IOException {
        this.in = new RandomAccessFile( activeFile, "r");
        this.activeFile =  activeFile.getName();
        this.outFile = new FileOutputStream( activeFile, true);
        this.out = new DataOutputStream(outFile);
        this.index = new ConcurrentHashMap<String, IndexRecord>();
    }

    public static HashIndexStore buildFrom(File dataFile) throws EOFException, IOException {
        final HashIndexStore store = new HashIndexStore(dataFile);
        walkFile(dataFile, new IndexVisitor() {
            public void visit(String key, IndexRecord record) throws IOException {
                store.index.put(key, record);
            }
        });
        return store;
    }


    public String get(String key) throws IOException {
        IndexRecord record = index.get(key);
        if (record == null) {
            return null;
        }

        in.seek(record.valueOffset);

        byte[] value = new byte[record.valueLength];
        in.readFully(value);

        return new String(value);
    }

    public void put(String key, String value) throws IOException {
        out.writeInt(key.length());
        out.write(key.getBytes());
        int valueLength = value.getBytes().length;

        out.writeInt(valueLength);
        out.write(value.getBytes());
        offset += 8 + key.getBytes().length;

        index.put(key, new IndexRecord(this.activeFile, valueLength, offset));

        offset += valueLength;
    }

    public void close() throws Exception {
        in.close();
        outFile.close();
    }

    private static class IndexRecord {
        String fileName;
        int valueLength;
        int valueOffset;

        IndexRecord(String fileName, int valueLength, int valueOffset) {
            this.fileName = fileName;
            this.valueLength = valueLength;
            this.valueOffset = valueOffset;
        }
    }

    private static void walkFile(File dataFile, IndexVisitor visitor) throws IOException {
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

    public static interface IndexVisitor {
        public void visit(String key, IndexRecord record) throws IOException;
    }

    public static class CorruptedDatabaseFileException extends RuntimeException {
        public CorruptedDatabaseFileException(String errorMessage) {
            super(errorMessage);
        }
    }
}
