package db.datastore.index;

import java.nio.file.Path;

public class BTree {

    private int order;

    private BTreeNode root;

    private BTree() {
    }

    public Rid search(int key) {
        return root.search(key);
    }

    public static BTree deserialize(Path file) {
        return null;
    }

    public BTreeNode getRoot() {
        return root;
    }

    public int getOrder() {
        return order;
    }
}
