package store;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class HintFile {

    private File file;
    private Map<String, IndexRecord> index;

    public HintFile(File file) {
        this.file = file;
    }

    public HintFile(File file, Map<String, IndexRecord> index) {
        this.file = file;
        this.index = index;
    }

    public boolean load(Map<String, IndexRecord> index) throws IOException {
        if (!file.exists()) {
            return false;
        }

        if (!loadIndex()) {
            return false;
        }

        for (Map.Entry entry : this.index.entrySet()) {
            index.put((String) entry.getKey(), (IndexRecord) entry.getValue());
        }
        return true;
    }

    public boolean loadIndex() throws IOException {
        CRC32 crc = new CRC32();
        long crcValue;
        Map<String, IndexRecord> index = new HashMap<String, IndexRecord>();
        try (FileInputStream inputStream = new FileInputStream(file)) {
            DataInputStream reader = new DataInputStream(inputStream);
            crcValue = reader.readLong();

            int keyLength;
            int valueLength;
            String key;
            int valueOffset;

            while (true) {

                ByteArrayOutputStream crcBuf = new ByteArrayOutputStream();
                DataOutputStream crcBufWriter = new DataOutputStream(crcBuf);

                try {
                    keyLength = reader.readInt();
                } catch (EOFException _) {
                    if (crc.getValue() != crcValue) {
                        System.err.println("error loading hint file " + file.getName() + ": CRC does not match");
                        return false;
                    }
                    this.index = index;
                    return true;
                }

                valueLength = reader.readInt();
                byte[] keyB = new byte[keyLength];
                reader.readFully(keyB);
                key = new String(keyB);
                valueOffset = reader.readInt();

                crcBufWriter.writeInt(keyLength);
                crcBufWriter.writeInt(valueLength);
                crcBufWriter.write(keyB);
                crcBufWriter.writeInt(valueOffset);
                crc.update(crcBuf.toByteArray());

                index.put(key, new IndexRecord(file.getName().replace(".hint", ""), valueLength, valueOffset));

            }
        }

    }

    public void save() throws IOException {
        file.createNewFile();
        CRC32 crc = new CRC32();
        try (FileOutputStream outputStream = new FileOutputStream(file, false)) {
            DataOutputStream writer = new DataOutputStream(outputStream);

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream bufWriter = new DataOutputStream(buf);
            // write the space where the CRC will go once it's been computed
            writer.writeLong(0);

            for (Map.Entry entry : index.entrySet()) {
                String key = (String) entry.getKey();
                IndexRecord record = (IndexRecord) entry.getValue();

                bufWriter.writeInt(key.getBytes().length);
                bufWriter.writeInt(record.valueLength);
                bufWriter.write(key.getBytes());
                bufWriter.writeInt(record.valueOffset);
                crc.update(buf.toByteArray());

                buf.writeTo(writer);
                buf.reset();
            }

        }
        // Write the checksum at the beginning of the file
        try (RandomAccessFile writer = new RandomAccessFile(file, "rw")) {
            writer.seek(0);
            writer.writeLong(crc.getValue());
        }
    }

    public Map<String, IndexRecord> getIndex() {
        return this.index;
    }
}
