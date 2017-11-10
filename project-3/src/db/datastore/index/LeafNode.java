package db.datastore.index;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A B+ Tree leaf node
 * <p>
 * Holds all data entries associated with this node
 */
public class LeafNode implements BTreeNode {

    private List<DataEntry> dataEntries;

    public LeafNode(List<DataEntry> entries) {
        this.dataEntries = entries;
    }

    /**
     * Retrieve data entry with specified key
     * @return The corresponding data entry, or null if not found
     */
    public DataEntry search(int key) {
        for (DataEntry entry : dataEntries) {
            if (entry.key == key) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Retrieve the lowest search key in this Node, useful to build search indexes
     *
     * @return minimumu value of DataEntry key (ie. attribute value) in node
     */
    public int getLowestKey() {
        // entries are sorted by key : just return first one
        return dataEntries.get(0).key;
    }

    /**
     * Read a leaf node from buffer page
     *
     * @param buffer Readable buffer containing a serialized leaf node
     */
    public static LeafNode deserialize(IntBuffer buffer) {
        // Check flag
        boolean isLeafNode = buffer.get() == 0;
        assert isLeafNode;

        int nbEntries = buffer.get();
        List<DataEntry> entries = new ArrayList<>(nbEntries);

        for (int i = 0; i < nbEntries; i++) {
            int key = buffer.get();
            int nbRids = buffer.get();

            Rid[] rids = new Rid[nbRids];

            for (int j = 0; j < nbRids; j++) {
                int pageid = buffer.get();
                int tupleid = buffer.get();

                rids[j] = new Rid(pageid, tupleid);
            }

            entries.add(new DataEntry(key, rids));
        }

        return new LeafNode(entries);
    }

    /**
     * Write a leaf node to a buffer page, filling empty space with 0
     *
     * @param buffer Writable byte buffer large enough to serialize node
     */
    @Override
    public void serialize(ByteBuffer buffer) {
        // Write flag
        buffer.putInt(0);

        buffer.putInt(dataEntries.size());

        for (DataEntry entry : dataEntries) {
            buffer.putInt(entry.key);
            buffer.putInt(entry.rids.length);

            for (Rid rid : entry.rids) {
                buffer.putInt(rid.pageid);
                buffer.putInt(rid.tupleid);
            }
        }

        // Fill remaining space with zeros
        while (buffer.hasRemaining()) {
            buffer.putInt(0);
        }
    }

    public List<DataEntry> getDataEntries() {
        return dataEntries;
    }
}
