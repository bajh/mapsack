package store;

import java.io.IOException;

public interface Store extends AutoCloseable {
    public String get(String key) throws IOException;
    public void put(String key, String value) throws IOException;
    public void delete(String key) throws IOException;
}
