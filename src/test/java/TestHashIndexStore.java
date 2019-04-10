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
        HashIndexStore store2 = new HashIndexStore(dataDir);
        store2.loadIndex();
        KeyTest keyTest3 = new KeyTest("key3", "hello from segment 2");
        store2.put(keyTest3.key, keyTest3.expectedValue);
        keyTest1.evaluate(store2);
        keyTest2.evaluate(store2);
        keyTest3.evaluate(store2);

        Thread.sleep(1);
        HashIndexStore store3 = new HashIndexStore(dataDir);
        store3.loadIndex();
        KeyTest keyTest4 = new KeyTest("key4", "hello from segment 3");
        store3.put(keyTest4.key, keyTest4.expectedValue);
        keyTest1.evaluate(store3);
        keyTest2.evaluate(store3);
        keyTest3.evaluate(store3);
        keyTest4.evaluate(store3);

        assertEquals(dataDir.listFiles().length, 3,
                "3 segment files should be created");
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
                    "expected value for " + key + " to be set to " + " but got " + actualValue);
        }
    }

}
