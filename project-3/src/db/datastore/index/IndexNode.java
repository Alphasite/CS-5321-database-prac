package db.datastore.index;

import java.util.ArrayList;
import java.util.List;

public class IndexNode implements BTreeNode {

    int[] keys;
    List<BTreeNode> children;

    public IndexNode(int nbKeys) {
        this.keys = new int[nbKeys];
        this.children = new ArrayList<>(nbKeys + 1);
    }

    @Override
    public Rid search(int key) {
        // TODO: implement binary search ?
        for (int i = 0; i < keys.length; i++) {
            if (key < keys[i]) {
                return children.get(i).search(key);
            }
        }

        return children.get(keys.length).search(key);
    }

    public static IndexNode deserialize() {

    }

    public void serialize() {

    }

}
