package db.datastore.index;

import db.datastore.Database;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

/**
 * B+ Tree class for indexing. Uses a tree of index nodes to efficiently access data records in leaf nodes.
 * <p>
 * Only the root node is kept in memory, all other nodes are lazily loaded from disk when requested.
 */
public class BTree {

    private int order;
    private int nbLeaves;

    private IndexNode root;

    private FileChannel channel;

    private BTree(int order, IndexNode root) {
        this.order = order;
        this.root = root;
    }

    private BTree(FileChannel channel, int order, int nbLeaves, int rootIndex) {
        this.channel = channel;

        this.order = order;
        this.nbLeaves = nbLeaves;

        this.root = (IndexNode) readNode(rootIndex);
    }

    /**
     * Build a new tree backed by specified file. Only header page and root node are deserialized.
     */
    public static BTree createTree(Path file) {
        try {
            FileChannel channel = FileChannel.open(file);
            ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

            // Read header page
            channel.read(buf);

            buf.flip();
            int rootAddress = buf.getInt();
            int nbLeaves = buf.getInt();
            int order = buf.getInt();

            return new BTree(channel, order, nbLeaves, rootAddress);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Read a specific node from index file
     *
     * @param indexInFile Page index in file
     * @return A LeafNode or IndexNode read from file
     */
    private BTreeNode readNode(int indexInFile) {
        ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);
        try {
            channel.read(buf, indexInFile * Database.PAGE_SIZE);
            buf.flip();

            if (indexInFile <= nbLeaves) {
                return LeafNode.deserialize(buf.asIntBuffer());
            } else {
                return IndexNode.deserialize(buf.asIntBuffer());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Search for the data entry with specified key in the tree structure.
     * Corresponding index and leaf pages will be loaded from disk when needed.
     *
     * @param key The search key
     * @return The corresponding data entry, or null if not found
     */
    public DataEntry search(int key) {
        IndexNode currentNode = root;
        LeafNode leafNode = null;

        // Kind of ugly but should work...
        while (leafNode == null) {
            int nextNode = currentNode.search(key);

            BTreeNode next = readNode(nextNode);

            if (next instanceof IndexNode) {
                currentNode = (IndexNode) next;
            } else {
                leafNode = (LeafNode) next;
            }
        }

        return leafNode.get(key);
    }

    public BTreeDataIterator iteratorForRange(Integer low, Integer high) {
        return new BTreeDataIterator(low, high);
    }

    /**
     * Save tree structure to file according to specification.
     * <p>
     * Pages are laid out as follows :
     * <ul>
     * <li>Header page with layout info</li>
     * <li>Leaf nodes in left-to-right order</li>
     * <li>First index node layer in left-to-right order</li>
     * <li>...</li>
     * <li>Root node</li>
     * </ul></p>
     *
     * @param file path to file
     */
    public void serialize(Path file) {

    }

    public IndexNode getRoot() {
        return root;
    }

    public int getOrder() {
        return order;
    }

    public int getNbLeaves() {
        return nbLeaves;
    }

    public class BTreeDataIterator implements Iterator<DataEntry> {

        private Integer low;
        private Integer high;
        private int currNodeAddr;
        private Iterator<DataEntry> currNodeIterator;

        public BTreeDataIterator(Integer low, Integer high) {
            this.low = low;
            this.high = high;

            if (this.low == null) {
                this.currNodeAddr = 1; // one after the header page

                List<DataEntry> dataEntries = ((LeafNode) readNode(this.currNodeAddr)).getDataEntries();
                this.currNodeIterator = dataEntries.listIterator();
            } else {
                IndexNode currentNode = root;
                int nextNodeAddr = currentNode.search(low);

                while (nextNodeAddr > nbLeaves) {
                    currentNode = (IndexNode) readNode(nextNodeAddr);
                    nextNodeAddr = currentNode.search(low);
                }

                this.currNodeAddr = nextNodeAddr;

                // get iterator starting from low or the smallest key bigger than low
                List<DataEntry> dataEntries = ((LeafNode) readNode(this.currNodeAddr)).getDataEntries();

                for (int i = 0; i < dataEntries.size(); i++) {
                    if (dataEntries.get(i).key >= low) {
                        this.currNodeIterator = dataEntries.listIterator(i);
                        break;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public DataEntry next() {
            return null;
        }
    }
}
