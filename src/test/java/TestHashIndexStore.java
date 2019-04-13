import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.TemporaryFolder;
import store.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;

@EnableRuleMigrationSupport
public class TestHashIndexStore {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testHashIndexStoreOperations() throws Exception {
        File dataDir = tempFolder.newFolder();
        HashIndexStore store = new HashIndexStore(dataDir);
        store.loadIndex();

        KeyTest keyTest1 = new KeyTest("key1", "hellooooooo");
        KeyTest keyTest2 = new KeyTest("key2", "goodbyeeee");
        store.put(keyTest1.key, keyTest1.value);
        store.put(keyTest2.key, keyTest2.value);

        keyTest1.evaluate(store);
        keyTest2.evaluate(store);

        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.loadIndex();
        KeyTest keyTest3 = new KeyTest("key3", "hello from segment 2");
        store.put(keyTest3.key, keyTest3.value);
        keyTest1.evaluate(store);
        keyTest2.evaluate(store);
        keyTest3.evaluate(store);

        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.loadIndex();
        KeyTest keyTest4 = new KeyTest("key4", "hello from segment 3");
        store.put(keyTest4.key, keyTest4.value);
        keyTest1.evaluate(store);
        keyTest2.evaluate(store);
        keyTest3.evaluate(store);
        keyTest4.evaluate(store);

        assertEquals(dataDir.listFiles().length, 3,
                "3 segment files should be created");
    }

    @Test
    public void testCompaction() throws Exception {
        File dataDir = tempFolder.newFolder();
        HashIndexStore store = new HashIndexStore(dataDir);
        store.loadIndex();
        KeyTest keyTest1 = new KeyTest("key1", "old value 1");
        KeyTest keyTest2 = new KeyTest("key2", "value 2");
        store.put(keyTest1.key, keyTest1.value);
        store.put(keyTest2.key, keyTest2.value);

        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.loadIndex();
        keyTest1 = new KeyTest("key1", "new value 1");
        store.put(keyTest1.key, keyTest1.value);
        KeyTest keyTest3 = new KeyTest("key3", "value 3");
        store.put(keyTest3.key, keyTest3.value);

        store.close();
        Thread.sleep(1);

        File[] segments = dataDir.listFiles();
        HashIndexStore.sortSegments(segments);

        store.compactSegments(segments[0], segments[1]);

        segments = dataDir.listFiles();
        assertEquals(1, segments.length, "expected 1 segment to remain after compaction");

        store = new HashIndexStore(dataDir);
        store.loadIndex();
        keyTest1.evaluate(store);
        keyTest2.evaluate(store);
        keyTest3.evaluate(store);

        // TODO: make sure compaction log gets updated...
        // TODO: make sure we don't try to compact active log file...
    }

    @Test
    public void testDeletion() throws Exception {
        File dataDir = tempFolder.newFolder();
        HashIndexStore store = new HashIndexStore(dataDir);
        store.loadIndex();

        KeyTest keyTest1 = new KeyTest("key 1", "val 1");
        store.put(keyTest1.key, keyTest1.value);
        KeyTest keyTest2 = new KeyTest("key 2", "val 2");
        store.put(keyTest2.key, keyTest2.value);
        store.delete(keyTest1.key);

        String value1 = store.get(keyTest1.key);
        assertEquals(value1, null, "expected deleted key to be not found");
        String value2 = store.get(keyTest2.key);
        keyTest2.evaluate(store);

        store.close();
        Thread.sleep(1);

        // After reloading data, record should still be gone
        store = new HashIndexStore(dataDir);
        store.loadIndex();
        value1 = store.get(keyTest1.key);
        assertEquals(value1, null, "expected deleted key to be not found after reloading data files");

        // Compaction should result in the key being removed from the created file
        File[] segments = dataDir.listFiles();
        store.compactSegments(segments[0], segments[1]);
        segments = dataDir.listFiles();
        Segment compacted = new Segment(segments[0]);
        compacted.walk(new Segment.Visitor() {
            public void visit(String key, IndexRecord record) throws IOException {
                if (key.equals(keyTest1.key)) {
                    assertEquals(false, true, "deleted key should not appear in compacted file");
                }
            }
        });
    }

    @Test
    public void testCorruptRecord() throws Exception {
        File dataDir = tempFolder.newFolder();
        File corruptFile = new File(Paths.get(dataDir.getAbsolutePath(), "corrupt").toString());
        corruptFile.createNewFile();
        ActiveSegment segment = new ActiveSegment(corruptFile);

        KeyTest keyTest1 = new KeyTest("key 1", "val 1");
        KeyTest keyTest2 = new KeyTest("key 2", "val 2 val 2 val 2 val 2");
        segment.put(keyTest1.key, keyTest1.value);
        segment.put(keyTest2.key, keyTest2.value);

        RandomAccessFile writer = new RandomAccessFile(corruptFile, "rw");
        writer.seek(corruptFile.length() - 8);
        writer.write("}:)".getBytes());

        segment.walk(new Segment.Visitor() {
            public void visit(String key, IndexRecord record) throws IOException {
                if (key == keyTest2.key) {
                    fail("expected corrupted key to be dropped from segment");
                }
            }
        });
    }

    @Test
    public void testTriggerSegmentSwitch() throws Exception {
        File dataDir = tempFolder.newFolder();

        HashIndexStore store = new HashIndexStore(dataDir);
        store.setMaximumFileSize(30);
        store.put("key1", "val1");
        File[] segments = dataDir.listFiles();
        if (segments.length != 1) {
            fail("expected one segment files to exist until file size threshold reached, found " + segments.length);
        }
        Thread.sleep(1);

        store.put("key2", "val2222222222222222222222222");

        segments = dataDir.listFiles();
        if (segments.length != 2){

            fail("expected two segment files to exist after file size threshold reached, got " + segments.length);
        }
    }

    @Test
    public void testScheduledCompaction() throws Exception {
        File dataDir = tempFolder.newFolder();

        HashIndexStore store = new HashIndexStore(dataDir);
        store.put("key1", "val1");
        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.put("key2", "val2");
        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.put("key3", "val3");
        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.put("key4", "val4");
        store.close();
        Thread.sleep(1);

        store = new HashIndexStore(dataDir);
        store.setCompactionPeriod(50);
        store.scheduleCompaction();
        Thread.sleep(80);

        File[] segments = dataDir.listFiles();
        if (segments.length != 2){
            fail("expected two segment files to exist after compaction performed, got " + segments.length);
        }

    }

    private class KeyTest {
        public String key;
        public String value;

        KeyTest(String key, String expectedValue) {
            this.key = key;
            this.value = expectedValue;
        }

        void evaluate(Store store) throws IOException {
            String actualValue = store.get(key);
            assertEquals(value, actualValue,
                    "expected value for " + key + " to be set to " + value + " but got " + actualValue);
        }
    }

}
