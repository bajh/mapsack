# MapSack

MapSack is a basic key-value store based on [BitCask](https://riak.com/assets/bitcask-intro.pdf).

The methods on the Store interface reveal exactly how simple it is:

```
public String get(String key) throws IOException;
public void put(String key, String value) throws IOException;
public void delete(String key) throws IOException;
```

`HashIndexStore` is the relevant implementation.

# Log-Based Storage

Data is written to log files called `Segment`s. These `Segment`s are ordered - currently this is done by giving them a timestamp as well as a counter that increases when segments are merged (more on this later). At any given time, only one `Segment` is being written to - this is the `ActiveSegment`.

Every time a put occurs, the key and value are appended to the `ActiveSegment`. `ActiveSegment.put` shows exactly how each record is serialized.

+======+==========+======================+============+===============+===================+=======================+
| SIZE | 64 bits  | 8 bits               |  32 bits   |   32 bits     | (key length) bits |  (value length) bits  |
+======+==========+======================+============+===============+===================+=======================+
| DATA | checksum | is this a tombstone? | key length |  value length |       key         |       value           |
+======+==========+======================+============+===============+===================+=======================+

These means our log file ends up being a whole bunch of these records stacked on top of each other, like this...

+==========+===+===+====+=======+===========================================+
| 0xdead   | 0 | 5 | 25 | user1 |       {"name": "Maria Theresa"}           |
+==========+===+===+====+====+==============================================+
| 0xbeef   | 0 | 5 | 16 | user2 |       {"name": "Otto"}                    |
+==========+============+=======+===============+===========================+
| 0xdeed   | 0 | 5 | 33 | user1 |       {"name": "Empress Maria Theresa"}   |
+==========+===+===+====+=======+===========================================+

Now, see here! There have been multiple writes to the key `user1`. The most recent one, with the name set to "Empress Maria Theresa", is the current one - "Maria Theresa" has been overwritten. This has two implications:

In order to `get` any of these values, we simply need the offset of the most recent record matching the key in the file, then we can use the value length to read exactly the data we need.

# In-Memory Index

In order to locate records in the `Segment` files, we keep an in-memory index in the form of a `Map`, which is found on the `index` field of `HashIndexStore` (hence the name MapSack). The Map allows us to go from a key to the `Segment` and `offset` where the value is stored. It's just that simple!

Of course, this means that all the keys and their offsets need to fit in memory. And whenever a write comes in, we need to update that `Map`.

The `HashIndexStore` has a method called `loadIndex` that is called at startup to go read in all the Segment files and initialize this `Map`.

# Recovery

But this means it could take a long time to start the server up while we're waiting to rebuild that `Map`! When we `compact` a segment (more on this below), we'll also create a `HintFile` for that segment, which is just a serialized form of the `Map` that can be loaded into memory faster.

# Segment Compaction

In our example above, the astute reader will have noticed that disk space is being wasted on keeping a record of user1 that has been overwritten. In order to address this, MapSack will periodically switch its `ActiveSegment`, sending futures writes a new file. Segments that are not being written to can then periodically be compacted and merged, resulting in files that have been cleaned of the unneeded prior values.

# Corruption Protection

What happens if a write is interrupted mid-way by a server crash, leaving one of our records only halfway written? In order to protect against this, each record is preceeded by a CRC value that is compared at startup time to the actual written value. If they don't match, the record is thrown away.

