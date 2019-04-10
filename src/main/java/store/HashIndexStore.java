package store;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    Map<String, IndexRecord> index;
    Segment activeSegment;

    public HashIndexStore(File activeFile) throws IOException {
        activeSegment = new Segment(activeFile);
        this.index = new ConcurrentHashMap<String, IndexRecord>();
    }

    public void loadIndex() throws IOException {
        // TODO: load all the segments in order, then create a new file for this segment
        activeSegment.load(index);
    }

    public String get(String key) throws IOException {
        IndexRecord record = index.get(key);
        if (record == null) {
            return null;
        }

        // TODO: find the correct segment based on record.fileName
        return activeSegment.get(record);
    }

    public void put(String key, String value) throws IOException {
        IndexRecord record = activeSegment.put(key, value);

        index.put(key, record);
    }

    public void close() throws Exception {
        activeSegment.close();
    }

}
