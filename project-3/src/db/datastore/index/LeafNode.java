package db.datastore.index;

import java.util.List;

public class LeafNode implements BTreeNode {

    private List<DataEntry> dataEntries;

    public class DataEntry {
        public int key;
        public Rid[] rids;

        public DataEntry(int key, Rid[] rids) {
            this.key = key;
            this.rids = rids;
        }
    }

    public LeafNode() {

    }

    @Override
    public Rid search(int key) {
        for (DataEntry entry : dataEntries) {
            if (entry.key >= key) {
                return entry.rids[0];
            }
        }
        return null;
    }

    public static LeafNode deserialize() {

    }

    public void serialize() {

    }

    public List<DataEntry> getDataEntries() {
        return dataEntries;
    }
}
