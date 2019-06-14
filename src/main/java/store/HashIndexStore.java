package store;

import java.io.*;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HashIndexStore implements Store, AutoCloseable {
    private Map<String, IndexRecord> index;
    File dataDir;
    private ActiveSegment activeSegment;
    TimerTask switchSegmentTask;

    private int maximumFileSize = 1024 * 1000;
    private long compactionPeriod = 1000L * 60L * 30L;

    private Map<String, Segment>  segments = new HashMap<String, Segment>();

    public HashIndexStore(File dataDir) throws Exception {
        this.dataDir = dataDir;
        this.activeSegment = new ActiveSegment(newSegmentFile());
        this.setIndex(new ConcurrentHashMap<String, IndexRecord>());
    }

    public void scheduleCompaction() {
        switchSegmentTask = new TimerTask() {
            public void run() {
                try {
                    doCompaction();
                } catch (Exception e) {
                    System.err.println("could not perform compaction");
                    e.printStackTrace();
                }
            }
        };

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(switchSegmentTask, compactionPeriod, compactionPeriod);
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

    private Segment getSegment(String segmentFile) throws IOException {
        if (segments.containsKey(segmentFile)) {
            return segments.get(segmentFile);
        }
        Segment segment = new Segment(getSegmentFile(segmentFile));
        segments.put(segmentFile, segment);
        return segment;
    }

    private Segment getSegment(File segmentFile) throws IOException {
        return getSegment(segmentFile.getName());
    }

    public void loadIndex() throws IOException {
        File[] segments = dataDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile() && !pathname.getName().endsWith("hint");
            }
        });
        HashIndexStore.sortSegments(segments);

        // load all the segments in order, then create a new file for this segment
        LocalTime loadStartTime = LocalTime.now();

        for (File segmentFile : segments) {
            try {
                HintFile hint = new HintFile(new File(segmentFile.getAbsolutePath() + ".hint"));
                if (hint.load(index)) {
                    System.out.println("loaded segment file " + segmentFile.getName() + " from hint file");
                    continue;
                }
                System.out.println("loaded segment file " + segmentFile.getName() + " from index file");
            } catch (IOException e) {
                System.err.println("unable to load hint for segment " + segmentFile.getName() + ". Using data file");
                e.printStackTrace();
            }

            Segment segment = getSegment(segmentFile);
            segment.load(index);
        }
        Duration loadTime = Duration.between(loadStartTime, LocalTime.now());

        System.out.printf("loaded segments in %s\n", loadTime);
    }

    public String get(String key) throws IOException {
        IndexRecord record = index.get(key);
        if (record == null) {
            return null;
        }

        // TODO: probably have a cache of the segments that exist?
        return getSegment(record.fileName).get(record);
    }

    public void put(String key, String value) throws IOException {
        IndexRecord record = activeSegment.put(key, value);

        index.put(key, record);
        if (activeSegment.getSize() > maximumFileSize) {
            setNewActiveSegment();
        }
    }

    public void delete(String key) throws IOException {
        activeSegment.delete(key);
        index.remove(key);
    }

    private File[] getSegmentFiles(boolean includeActive) throws IOException {
        File[] segmentFiles = dataDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (includeActive) {
                    return !name.endsWith("hint");
                }
                return !name.equals(activeSegment.getFileName()) && !name.endsWith("hint");
            }
        });
        HashIndexStore.sortSegments(segmentFiles);
        return segmentFiles;
    }

    // doCompaction compacts two segments at a time until they've all been compacted
    public void doCompaction() throws Exception {
        File[] segmentFiles = getSegmentFiles(false);

        if (segmentFiles.length > 1) {
            File leftMergeFile = null;
            for (File segmentFile : segmentFiles) {
                if (leftMergeFile == null) {
                    leftMergeFile = segmentFile;
                    continue;
                }

                leftMergeFile = compactSegments(leftMergeFile, segmentFile);
            }
        }

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

    public File compactSegments(File oldSegment, File newSegment) throws IOException {
        Map<String, IndexRecord> index = new HashMap<String, IndexRecord>();
        Map<String, IndexRecord> hintIndex = new HashMap<String, IndexRecord>();
        Map<String, Segment> segments = new HashMap<String, Segment>();

        Segment segment1 = getSegment(oldSegment);
        Segment segment2 = getSegment(newSegment);
        segments.put(segment1.dataFile.getName(), segment1);
        segments.put(segment2.dataFile.getName(), segment2);
        segment1.load(index);
        segment2.load(index);

        File compactedFile = compactedSegmentFile(oldSegment, newSegment);
        compactedFile.createNewFile();

        ActiveSegment outputSegment = new ActiveSegment(compactedFile);
        for (Map.Entry entry : index.entrySet()) {
            IndexRecord record = (IndexRecord) entry.getValue();
            Segment recordSegment = segments.get(record.fileName);
            String value = recordSegment.get(record);

            String key = (String) entry.getKey();
            IndexRecord newSegmentRecord = outputSegment.put(key, value);
            hintIndex.put(key, newSegmentRecord);
        }

        // update the segment file names offset map - this seems expensive, not sure if there's a better way to do this
        for (Map.Entry entry : hintIndex.entrySet()) {
            IndexRecord record = this.index.get(entry.getKey());
            if (record != null) {
                IndexRecord newRecord = (IndexRecord) entry.getValue();
                record.mergeWith(newRecord);
            }
        }

        oldSegment.delete();
        newSegment.delete();

        HintFile hintFile = new HintFile(new File(compactedFile.getAbsolutePath() + ".hint"), hintIndex);
        hintFile.save();
        // delete old hint files if they exist
        File oldHintFile = new File(oldSegment.getAbsolutePath() + ".hint");
        oldHintFile.delete();

        return compactedFile;
    }

    private synchronized void setNewActiveSegment() throws IOException {
        File segmentFile = newSegmentFile();
        ActiveSegment segment = new ActiveSegment(segmentFile);
        this.activeSegment.close();
        this.activeSegment = segment;
    }

    public void close() throws IOException {
        activeSegment.close();
        if (switchSegmentTask != null) {
            switchSegmentTask.cancel();
        }
        for (Segment segment : segments.values()) {
            segment.close();
        }
    }

    public void setCompactionPeriod(long segmentSwitchPeriod) {
        this.compactionPeriod = segmentSwitchPeriod;
    }

    public void setMaximumFileSize(int maximumFileSize){
        this.maximumFileSize = maximumFileSize;
    }

    public void setIndex(Map<String, IndexRecord> index) {
        this.index = index;
    }
}
