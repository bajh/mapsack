package store;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ActiveSegment extends Segment {
    int offset;

    FileOutputStream outputStream;
    DataOutputStream writer;

    public ActiveSegment(File dataFile) throws IOException {
        super(dataFile);
        this.outputStream = new FileOutputStream(dataFile, true);
        this.writer = new DataOutputStream(outputStream);
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


    public void close() throws Exception {
        super.close();
        outputStream.close();
    }

}
