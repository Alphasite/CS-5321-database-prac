package db.datastore.index;

import java.nio.ByteBuffer;

public interface BTreeNode {

//    Rid search(int key);

//    void deserialize(IntBuffer buf);

    /**
     * Write node contents to buffer page
     *
     * @param buf Writable buffer
     */
    void serialize(ByteBuffer buf);
}
