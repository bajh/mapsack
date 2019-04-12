package store;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    Map<String, IndexRecord> index;
    // compactionLog holds a lookup table of k -> v that allows us to find out what
    // segment v the segment k was compacted to.
    Map<String, String> compactionLog = new ConcurrentHashMap<String, String>();
    File dataDir;
    ActiveSegment activeSegment;

    public HashIndexStore(File dataDir) throws IOException {
        this.dataDir = dataDir;
        activeSegment = new ActiveSegment(newSegmentFile());
        this.index = new ConcurrentHashMap<String, IndexRecord>();
    }

    public File getSegmentFile(String fileName) {
        String compactedFileName = compactionLog.get(fileName);

        // TODO: feels like there should be a better way to do this, though.
        // At the very least, if a segment is compacted multiple times we should
        // clean up the compactionLog so that we're guaranteed to only have to look
        // in it once for each segment. Then there's also the memory usage growth...
        if (compactedFileName != null) {
            return getSegmentFile(compactedFileName);
        }

        return Paths.get(dataDir.getAbsolutePath(), fileName).toFile();
    }

    public File newSegmentFile() throws IOException {
        long unixTime = System.currentTimeMillis();
        File segmentFile = getSegmentFile(Long.toString(unixTime));
        segmentFile.createNewFile();
        return segmentFile;
    }

    public static void sortSegments(File[] segments) {
        Arrays.sort(segments, new Comparator<File>() {
            public int compare(File f1, File f2) {

                String[] f1NameParts = f1.getName().split("-");
                // TODO: be defensive about the parsing in case someone threw a random file in here
                long f1Timestamp = Long.parseLong(f1NameParts[0]);

                String[] f2NameParts = f2.getName().split("-");
                long f2Timestamp = Long.parseLong(f2NameParts[0]);

                if (f1Timestamp == f2Timestamp) {
                    // TODO: need to check that the name parts are both length 2
                    // otherwise we're dealing with a corrupt dataDir and can't choose which one to use

                    return Integer.compare(Integer.parseInt(f1NameParts[1]), Integer.parseInt(f2NameParts[1]));
                }

                return Long.compare(f1Timestamp, f2Timestamp);
            }
        });
    }

    public void loadIndex() throws IOException {
        File[] segments = dataDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile();
            }
        });
        HashIndexStore.sortSegments(segments);

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

    public void delete(String key) throws IOException {
        activeSegment.delete(key);
        index.remove(key);
    }

    // compactSegments randomly chooses two inactive segments to compact and does
    // so
    public void doCompaction() throws Exception {
        File[] segmentFiles = dataDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.equals(activeSegment.getFileName());
            }
        });

        Random rand = new Random();
        File segment1 = segmentFiles[rand.nextInt(segmentFiles.length)];
        File segment2 = segmentFiles[rand.nextInt(segmentFiles.length)];

        compactSegments(segment1, segment2);
    }

    // TODO: this function needs to be defensive against files that are named wrongly
    // TODO: also, I'm not sure if it should assume it's receiving the files in the correct order, maybe it should sort them because callers may not realize this expectation exists about the arguments?
    private File compactedSegmentFile(File oldFile, File newFile) {
        String[] newFileNameParts = newFile.getName().split("-");

        long newFileTimestamp = Long.parseLong(newFileNameParts[0]);
        int compactedFileVersion = 1;
        if (newFileNameParts.length == 2) {
            compactedFileVersion = compactedFileVersion + Integer.parseInt(newFileNameParts[1]);
        }
        return getSegmentFile(Long.toString(newFileTimestamp) + "-" + Integer.toString(compactedFileVersion));
    }

    public void compactSegments(File segmentFile1, File segmentFile2) throws IOException {
        Map<String, IndexRecord> index = new HashMap<String, IndexRecord>();
//        Map<String, IndexRecord> hintIndex = new HashMap<String, IndexRecord>();
        Map<String, Segment> segments = new HashMap<String, Segment>();
        File[] segmentFiles = new File[]{segmentFile1, segmentFile2};
        HashIndexStore.sortSegments(segmentFiles);

        Segment segment1 = new Segment(segmentFiles[0]);
        Segment segment2 = new Segment(segmentFiles[1]);
        segments.put(segment1.dataFile.getName(), segment1);
        segments.put(segment2.dataFile.getName(), segment2);
        segment1.load(index);
        segment2.load(index);

        File compactedFile = compactedSegmentFile(segmentFile1, segmentFile2);
        compactedFile.createNewFile();

        ActiveSegment outputSegment = new ActiveSegment(compactedFile);
        for (Map.Entry entry : index.entrySet()) {
            IndexRecord record = (IndexRecord) entry.getValue();
            Segment recordSegment = segments.get(record.fileName);
            String value = recordSegment.get(record);

            String key = (String) entry.getKey();
            IndexRecord newSegmentRecord = outputSegment.put(key, value);
            //hintIndex.put(key, newSegmentRecord);
        }

        compactionLog.put(segmentFile1.getName(), outputSegment.getFileName());
        compactionLog.put(segmentFile2.getName(), outputSegment.getFileName());

        segmentFile1.delete();
        segmentFile2.delete();

        // TODO: dumpHintFile(index, segmentFile2);
    }

    public void close() throws Exception {
        activeSegment.close();
    }

}
