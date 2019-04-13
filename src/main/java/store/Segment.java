package store;

import java.io.*;
import java.util.Map;
import java.util.zip.CRC32;

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
                long crcVal;
                try {
                    crcVal = din.readLong();
                } catch (EOFException _) {
                    return;
                }

                // TODO: we only actually need to check the CRC of the last record in the file, but not sure how to do that eloquently rn

                ByteArrayOutputStream crcBuf = new ByteArrayOutputStream();
                DataOutputStream crcBufWriter = new DataOutputStream(crcBuf);

                meta = din.readByte();
                keyLength = din.readInt();
                valueLength = din.readInt();

                crcBufWriter.writeByte(meta);
                crcBufWriter.writeInt(keyLength);
                crcBufWriter.writeInt(valueLength);

                byte[] keyBuf = new byte[keyLength];
                din.readFully(keyBuf);
                crcBufWriter.write(keyBuf);
                // TODO: don't have this magic number here
                // crc 8 + meta 1 + keylen 4 + vallen 4
                int valueOffset = 17 + keyLength + offset;

                byte[] valueBuf = new byte[valueLength];
                din.readFully(valueBuf);
                // TODO: I don't think I need to copy to this intermediate buffer, but not sure easier way atm
                crcBufWriter.write(valueBuf);

                offset = valueOffset + valueLength;

                CRC32 crc = new CRC32();
                crc.update(crcBuf.toByteArray());
                if (crc.getValue() != crcVal) {
                    // TODO: I'm not sure how this should actually be handled, here the record just gets unceremoniously dropped...
                    System.err.println("invalid CRC val");
                    return;
                }

                if (isTombstoneBitSet(meta)) {
                    visitor.visit(new String(keyBuf), null);
                    return;
                }
                visitor.visit(new String(keyBuf),
                    new IndexRecord(dataFile.getName(), valueLength, valueOffset));

            }

        }
    }

    public String getFileName() {
        return dataFile.getName();
    }

    public void close() throws IOException {
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
