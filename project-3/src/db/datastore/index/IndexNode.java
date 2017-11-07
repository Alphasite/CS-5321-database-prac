package db.datastore.index;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * A B+ Tree index node
 * <p>
 * Does not keep references to child nodes, only the page indices are stored
 */
public class IndexNode implements BTreeNode {

    int[] keys;
    int[] children;

    public IndexNode(int nbKeys) {
        this.keys = new int[nbKeys];
        this.children = new int[nbKeys + 1];
    }

    public IndexNode(int[] keys, int[] children) {
        assert children.length == (keys.length + 1);

        this.keys = keys;
        this.children = children;
    }

    /**
     * B+ Tree index search for specified key. Returned child index c[i] is such that  keys[i-1] <= c[i] < keys[i]
     *
     * @return The index of the next node in search path
     */
    public int search(int key) {
        // TODO: implement binary search ?
        for (int i = 0; i < keys.length; i++) {
            if (key < keys[i]) {
                return children[i];
            }
        }

        return children[keys.length];
    }

    /**
     * Read an index node from buffer page
     *
     * @param buffer Readable buffer containing a serialized index node
     */
    public static IndexNode deserialize(IntBuffer buffer) {
        // Check flag
        assert buffer.get() == 1;

        int nbKeys = buffer.get();

        IndexNode node = new IndexNode(nbKeys);

        for (int i = 0; i < nbKeys; i++) {
            node.keys[i] = buffer.get();
        }

        for (int i = 0; i < nbKeys + 1; i++) {
            node.children[i] = buffer.get();
        }

        return node;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        // Write flag
        buffer.putInt(1);

        buffer.putInt(keys.length);

        for (int key : keys) {
            buffer.putInt(key);
        }

        for (int child : children) {
            buffer.putInt(child);
        }

        // Fill remaining space with zeros
        while (buffer.hasRemaining()) {
            buffer.putInt(0);
        }
    }

}
