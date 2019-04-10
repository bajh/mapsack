package store;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    Map<String, Integer> index;
    RandomAccessFile in;
    FileOutputStream outFile;
    DataOutput out;
    int offset;

    public HashIndexStore(File dataFile) throws IOException {
        this.in = new RandomAccessFile(dataFile, "r");
        this.outFile = new FileOutputStream(dataFile, true);
        this.out = new DataOutputStream(outFile);
        this.index = new ConcurrentHashMap<String, Integer>();
    }

    public static HashIndexStore buildFrom(File dataFile) throws EOFException, IOException {
        int keyLength;
        int valueLength;
        int offset = 0;

        FileInputStream fis = new FileInputStream(dataFile);
        DataInputStream din = new DataInputStream(fis);

        HashIndexStore store = new HashIndexStore(dataFile);
        while (true) {
            try {
                keyLength = din.readInt();
            } catch (EOFException _) {
                return store;
            }
            // TODO: instead of letting the EOFException get surfaced, decorate it with a better/more specific Exception

            byte[] keyBuf = new byte[keyLength];
            din.readFully(keyBuf);
            offset += 4 + keyLength;

            store.index.put(new String(keyBuf), offset);

            valueLength = din.readInt();
            offset += 4;

            din.skipBytes(valueLength);
            offset += valueLength;
        }
    }


    public String get(String key) throws IOException {
        Integer offset = index.get(key);
        if (offset == null) {
            return null;
        }

        in.seek(offset);

        // TODO: probably format this error better
        int valueLength = in.readInt();
        byte[] value = new byte[valueLength];
        in.readFully(value);

        return new String(value);
    }

    public void put(String key, String value) throws IOException {
        out.writeInt(key.length());
        out.write(key.getBytes());
        offset += 4 + key.getBytes().length;

        out.writeInt(value.length());
        out.write(value.getBytes());

        index.put(key, offset);

        offset += 4 + value.getBytes().length;
    }

    public void close() throws Exception {
        in.close();
        outFile.close();
    }

    public static class CorruptedDatabaseFileException extends RuntimeException {
        public CorruptedDatabaseFileException(String errorMessage) {
            super(errorMessage);
        }
    }
}
