import server.DBServer;
import store.HashIndexStore;

import java.io.File;

public class HashIndexDB {
    public static void main(String[] args) throws Exception {
        // TODO: replace hardcoded filename

        File dataDir = new File("./datafile");
        dataDir.mkdir();

        try (HashIndexStore store = new HashIndexStore(dataDir)) {
            store.loadIndex();
            DBServer server = new DBServer(store);
            server.run();
        }

    }

}
