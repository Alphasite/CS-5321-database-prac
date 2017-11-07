package db.datastore.index;

import db.datastore.Database;
import db.datastore.IndexInfo;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.operators.physical.Operator;
import db.operators.physical.extended.InMemorySortOperator;
import db.operators.physical.physical.ScanOperator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class BulkLoader {

    private TableInfo tableInfo;
    private String attributeName;
    private int attributeIndex;

    private int treeOrder;
    private boolean clustered;

    private Path file;
    private FileChannel output;

    private BulkLoader(TableInfo target, IndexInfo parameters, Path file) {
        this.tableInfo = target;
        this.attributeName = parameters.attributeName;

        this.attributeIndex = target.header.resolve(target.tableName, attributeName).get();

        this.treeOrder = parameters.treeOrder;
        this.clustered = parameters.isClustered;

        this.file = file;
        this.output = null;
    }

    /**
     * Build a serialized B+ Tree index on a table in specified folder.
     *
     * @param DB
     * @param parameters
     * @param folder
     * @return The file that was created to store the index tree
     */
    public static Path buildIndex(Database DB, IndexInfo parameters, Path folder) {
        Path outputFile = folder.resolve(parameters.tableName + "." + parameters.attributeName);

        BulkLoader builder = new BulkLoader(DB.getTable(parameters.tableName), parameters, outputFile);
        builder.build();

        return outputFile;
    }

    public void build() {
        if (clustered) {
            sortRelation();
        }

        BinaryTupleReader input = BinaryTupleReader.get(tableInfo.file);
        List<DataEntry> entries = loadDataEntries(input);
        input.close();

        try {
            this.output = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

            // We need to keep the nodes in memory to generate the index keys
            List<LeafNode> leafNodes = buildLeafNodes(entries);

            int nbNodes = buildIndexNodes(leafNodes);

            writeHeader(leafNodes.size(), nbNodes);

            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read the table from file, sort its contents according to index key (with unbounded state) and then write it back
     */
    private void sortRelation() {
        Operator scan = new ScanOperator(tableInfo);
        TableHeader sortHeader = new TableHeader(Arrays.asList(tableInfo.tableName), Arrays.asList(attributeName));
        Operator sortOp = new InMemorySortOperator(scan, sortHeader);

        Path tempSortOutput = tableInfo.file.resolveSibling(tableInfo.tableName + ".tmp");
        BinaryTupleWriter writer = BinaryTupleWriter.get(tableInfo.header, tempSortOutput);
        sortOp.dump(writer);
        writer.close();
        sortOp.close();

        // Replace old file with new one
        try {
            Files.move(tempSortOutput, tableInfo.file, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Read tuples from input and generate data entries with the required format :
     * {@link Rid} array sorted by pageid, tupleid</li>
     *
     * @param reader Tuple source
     * @return List of {@link DataEntry} sorted by search key (ie. indexed attribute value)
     */
    private List<DataEntry> loadDataEntries(BinaryTupleReader reader) {
        Map<Integer, List<Rid>> entryMap = new HashMap<>();

        int pageId = 0;
        int tupleId = 0;

        Tuple tuple;
        while ((tuple = reader.next()) != null) {
            Rid rid = new Rid(pageId, tupleId);
            int key = tuple.fields.get(attributeIndex);

            if (!entryMap.containsKey(key)) {
                entryMap.put(key, new ArrayList<>());
            }
            entryMap.get(key).add(rid);

            tupleId++;
            if (tupleId == reader.getNumberOfTuples()) {
                // Mark as new page
                tupleId = 0;
                pageId++;
            }
        }

        reader.close();

        // Sort by key and generate data entries
        List<DataEntry> entries = new ArrayList<>(entryMap.size());
        entryMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach((e) -> {
                    Rid[] arr = e.getValue().toArray(new Rid[e.getValue().size()]);
                    Arrays.sort(arr);
                    entries.add(new DataEntry(e.getKey(), arr));
                });

        return entries;
    }

    /**
     * Serialize data entries as leaf nodes on disk, overwriting contents of pages required starting from number one.
     *
     * @param entries data entries to serialize
     * @return The generated leaf nodes
     */
    private List<LeafNode> buildLeafNodes(List<DataEntry> entries) {
        int currentPage = 1;
        int currentIndex = 0;
        int entriesRemaining = entries.size();
        int d = treeOrder;

        List<LeafNode> nodes = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        while (entriesRemaining > 0) {
            int entriesToWrite;

            // Prevent underfilling of last node
            if (2 * d < entriesRemaining && entriesRemaining < 3 * d) {
                entriesToWrite = entriesRemaining / 2;
            } else {
                entriesToWrite = Math.min(2 * d, entriesRemaining);
            }

            assert entriesToWrite >= d && entriesToWrite <= 2 * d;

            LeafNode node = new LeafNode(entries.subList(currentIndex, currentIndex + entriesToWrite));

            // node is responsible for zeroing free space
            node.serialize(buf);
            buf.flip();

            try {
                // Write this node as a page in file
                output.write(buf, currentPage * Database.PAGE_SIZE);
                buf.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }

            nodes.add(node);

            currentIndex += entriesToWrite;
            entriesRemaining -= entriesToWrite;

            currentPage++;
        }

        return nodes;
    }

    /**
     * @param leafNodes
     * @return total number of nodes serialized to file (including leaves)
     */
    private int buildIndexNodes(List<LeafNode> leafNodes) {
        int currentPage = leafNodes.size() + 1;
        int nodesInPrevLayer = leafNodes.size();
        int d = treeOrder;

        //
        int lowerRange = 1;
        int upperRange = leafNodes.size();
        int current = lowerRange;

        // Generate map associating each node to the minimal search key in corresponding subtree
        Map<Integer, Integer> keyMap = new HashMap<>();
        // Since Data entries are sorted by key we can read first entry in each leaf
        for (int i = 0; i < leafNodes.size(); i++) {
            keyMap.put(i + 1, leafNodes.get(i).getDataEntries().get(0).key);
        }

        ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        while (nodesInPrevLayer > 2 * d + 1) {
            // Build an index layer
            int nodesInLayer = 0;
            int nodesRemaining = nodesInPrevLayer;

            while (nodesRemaining > 0) {
                // Generate index node
                int nodesToWrite;

                // Prevent underfilling of last node
                if (2 * d + 1 < nodesRemaining && nodesRemaining < 3 * d + 2) {
                    nodesToWrite = nodesRemaining / 2;
                } else {
                    nodesToWrite = Math.min(2 * d + 1, nodesRemaining);
                }

                assert nodesToWrite >= d + 1 && nodesToWrite <= 2 * d + 1;

                int[] children = range(current, current + nodesToWrite);
                int[] keys = generateKeys(keyMap, children);
                keyMap.put(currentPage, leafNodes.get(children[0] - 1).getDataEntries().get(0).key);

                writeIndexNode(keys, children, buf, currentPage);

                nodesRemaining -= nodesToWrite;
                current += nodesToWrite;

                nodesInLayer++;
                currentPage++;
            }

            nodesInPrevLayer = nodesInLayer;

            lowerRange = upperRange + 1;
            upperRange = lowerRange + nodesInPrevLayer - 1;
        }

        // Build root node
        int[] children = range(lowerRange, upperRange + 1);
        int[] keys = generateKeys(keyMap, children);

        writeIndexNode(keys, children, buf, currentPage);

        return currentPage;
    }

    private int[] generateKeys(Map<Integer, Integer> keyMap, int[] children) {
        int[] keys = new int[children.length - 1];
        for (int i = 1; i < children.length; i++) {
            keys[i - 1] = keyMap.get(children[i]);
        }
        return keys;
    }

    private void writeIndexNode(int[] keys, int[] children, ByteBuffer buffer, int page) {
        IndexNode node = new IndexNode(keys, children);

        node.serialize(buffer);
        buffer.flip();

        try {
            output.write(buffer, page * Database.PAGE_SIZE);
            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return Array of integers [a, b[
     */
    private int[] range(int a, int b) {
        int[] res = new int[b - a];
        for (int i = 0; i < res.length; i++) {
            res[i] = a + i;
        }
        return res;
    }

    private void writeHeader(int nbLeaves, int rootAddress) {
        ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        buf.putInt(rootAddress);
        buf.putInt(nbLeaves);
        buf.putInt(treeOrder);
        buf.flip();

        try {
            output.write(buf, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
