import server.DBServer;
import store.HashIndexStore;
import store.Store;

import java.io.File;

public class HashIndexDB {
    public static void main(String[] args) throws Exception {
        // TODO: replace hardcoded filename
        File dataFile = new File("./datafile");
        dataFile.createNewFile();

        try (HashIndexStore store = new HashIndexStore(dataFile)) {
            store.loadIndex();
            DBServer server = new DBServer(store);
            server.start();
        }

    }
}
