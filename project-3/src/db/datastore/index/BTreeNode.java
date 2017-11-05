package db.datastore.index;

public interface BTreeNode {

    Rid search(int key);
}
