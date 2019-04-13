package store;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class ActiveSegment extends Segment {
    private AtomicInteger offset = new AtomicInteger(0);

    FileOutputStream outputStream;
    DataOutputStream writer;

    public ActiveSegment(File dataFile) throws IOException {
        super(dataFile);
        this.outputStream = new FileOutputStream(dataFile, true);
        this.writer = new DataOutputStream(outputStream);
    }

    private synchronized void writeRecord(CRC32 crc, ByteArrayOutputStream buf, int offsetChange) throws IOException {
        writer.writeLong(crc.getValue());
        buf.writeTo(writer);
        while (true) {
            int lastOffset = offset.get();
            int nextOffset = lastOffset + offsetChange;
            if (offset.compareAndSet(lastOffset, nextOffset)) {
                return;
            }
        }
    }

    public IndexRecord put(String key, String value) throws IOException {

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream bufWriter = new DataOutputStream(buf);

        bufWriter.writeByte(0);
        bufWriter.writeInt(key.length());
        int valueLength = value.getBytes().length;
        bufWriter.writeInt(valueLength);
        bufWriter.write(key.getBytes());
        bufWriter.write(value.getBytes());

        // TODO: make this not a magic number: it's the metadata length (1) + key length (4) + value length (4) + crc length (8)
        int offsetChange = 17 + key.getBytes().length;
        int valueOffset = offset.get() + offsetChange;

        CRC32 crc = new CRC32();
        crc.update(buf.toByteArray());
        writeRecord(crc, buf, offsetChange + valueLength);

        return new IndexRecord(dataFile.getName(), valueLength, valueOffset);
    }

    public void delete(String key) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream bufWriter = new DataOutputStream(buf);

        bufWriter.writeByte(1);
        bufWriter.writeInt(key.length());
        bufWriter.writeInt(0);
        bufWriter.write(key.getBytes());

        CRC32 crc = new CRC32();
        crc.update(buf.toByteArray());

        writeRecord(crc, buf, 17 + key.getBytes().length);
    }

    public int getSize() {
        return offset.get();
    }

    public void close() throws IOException {
        super.close();
        outputStream.close();
    }

}
