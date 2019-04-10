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
    public void testHashIndexStoreOperations() throws IOException {
        final File tempFile = tempFolder.newFile();
        HashIndexStore store = HashIndexStore.buildFrom(tempFile);

        KeyTest keyTest1 = new KeyTest("key1", "hellooooooo");
        KeyTest keyTest2 = new KeyTest("key2", "goodbyeeee");
        store.put(keyTest1.key, keyTest1.expectedValue);
        store.put(keyTest2.key, keyTest2.expectedValue);

        keyTest1.evaluate(store);
        keyTest2.evaluate(store);

        // test that rebuilding an existing database in memory works
        HashIndexStore store2 = HashIndexStore.buildFrom(tempFile);
        keyTest1.evaluate(store2);
        keyTest2.evaluate(store2);
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
