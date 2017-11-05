package db.datastore.index;

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
    public DataEntry get(int key) {
        for (DataEntry entry : dataEntries) {
            if (entry.key == key) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Read a leaf node from buffer page
     *
     * @param buffer Readable buffer containing a serialized leaf node
     */
    public static LeafNode deserialize(IntBuffer buffer) {
        // Check flag
        assert buffer.get() == 0;

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

    @Override
    public void serialize(IntBuffer buffer) {

    }

    public List<DataEntry> getDataEntries() {
        return dataEntries;
    }
}
