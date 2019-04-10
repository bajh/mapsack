package store;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    Map<String, IndexRecord> index;
    File dataDir;
    Segment activeSegment;

    public HashIndexStore(File dataDir) throws IOException {
        this.dataDir = dataDir;
        activeSegment = new Segment(newSegmentFile());
        this.index = new ConcurrentHashMap<String, IndexRecord>();
    }

    public File getSegmentFile(String fileName) {
        return Paths.get(dataDir.getAbsolutePath(), fileName).toFile();
    }

    public File newSegmentFile() throws IOException {
        long unixTime = System.currentTimeMillis();
        File segmentFile = getSegmentFile(Long.toString(unixTime));
        segmentFile.createNewFile();
        return segmentFile;
    }

    public void loadIndex() throws IOException {
        File[] segments = dataDir.listFiles();
        Arrays.sort(segments, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return Long.compare(Long.parseLong(f1.getName()), Long.parseLong(f2.getName()));
            }
        });

        // load all the segments in order, then create a new file for this segment
        for (File segmentFile : segments) {
            Segment segment = new Segment(segmentFile);
            segment.load(index);
        }
    }

    public String get(String key) throws IOException {
        IndexRecord record = index.get(key);
        if (record == null) {
            return null;
        }

        // TODO: probably have a cache of the segments that exist?
        return new Segment(getSegmentFile(record.fileName)).get(record);
    }

    public void put(String key, String value) throws IOException {
        IndexRecord record = activeSegment.put(key, value);

        index.put(key, record);
    }

    public void close() throws Exception {
        activeSegment.close();
    }

}
