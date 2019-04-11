import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.TemporaryFolder;
import store.HashIndexStore;
import store.Store;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;

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
        store.put(keyTest1.key, keyTest1.expectedValue);
        store.put(keyTest2.key, keyTest2.expectedValue);

        keyTest1.evaluate(store);
        keyTest2.evaluate(store);

        Thread.sleep(1);
        store = new HashIndexStore(dataDir);
        store.loadIndex();
        KeyTest keyTest3 = new KeyTest("key3", "hello from segment 2");
        store.put(keyTest3.key, keyTest3.expectedValue);
        keyTest1.evaluate(store);
        keyTest2.evaluate(store);
        keyTest3.evaluate(store);

        Thread.sleep(1);
        store = new HashIndexStore(dataDir);
        store.loadIndex();
        KeyTest keyTest4 = new KeyTest("key4", "hello from segment 3");
        store.put(keyTest4.key, keyTest4.expectedValue);
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
        store.put(keyTest1.key, keyTest1.expectedValue);
        store.put(keyTest2.key, keyTest2.expectedValue);
        store.close();

        store = new HashIndexStore(dataDir);
        store.loadIndex();
        keyTest1 = new KeyTest("key1", "new value 1");
        store.put(keyTest1.key, keyTest1.expectedValue);
        KeyTest keyTest3 = new KeyTest("key3", "value 3");
        store.put(keyTest3.key, keyTest3.expectedValue);
        store.close();

        File[] segments = dataDir.listFiles();

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

    private class KeyTest {
        public String key;
        public String expectedValue;

        KeyTest(String key, String expectedValue) {
            this.key = key;
            this.expectedValue = expectedValue;
        }

        void evaluate(Store store) throws IOException {
            String actualValue = store.get(key);
            assertEquals(expectedValue, actualValue,
                    "expected value for " + key + " to be set to " + expectedValue + " but got " + actualValue);
        }
    }

}
