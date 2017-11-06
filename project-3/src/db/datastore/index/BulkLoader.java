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
            this.output = FileChannel.open(file);

            buildLeafNodes(entries);

            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        int tuplesOnPage = reader.getNumberOfTuples();
        int tupleId = -1;

        Tuple tuple;
        while ((tuple = reader.next()) != null) {
            tupleId++;
            if (tupleId == tuplesOnPage) {
                tupleId = 0;
                pageId++;
                tuplesOnPage = reader.getNumberOfTuples();
            }

            Rid rid = new Rid(pageId, tupleId);
            int key = tuple.fields.get(attributeIndex);

            if (!entryMap.containsKey(key)) {
                entryMap.put(key, new ArrayList<>());
            }
            entryMap.get(key).add(rid);
        }

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

    private void buildLeafNodes(List<DataEntry> entries) {
        int currentPage = 1;
        int currentIndex = 0;
        int entriesRemaining = entries.size();
        int d = treeOrder;

        while (entriesRemaining > 0) {
            int entriesToWrite;

            // Prevent underfilling of last node
            if (2 * d < entriesRemaining && entriesRemaining < 3 * d) {
                entriesToWrite = entriesRemaining / 2;
            } else {
                entriesToWrite = Math.max(2 * d, entriesRemaining);
            }

            assert entriesToWrite >= d && entriesToWrite <= 2 * d;

            LeafNode node = new LeafNode(entries.subList(currentIndex, currentIndex + entriesToWrite));

            ByteBuffer buf = ByteBuffer.allocateDirect(Database.PAGE_SIZE);
            // node is responsible for zeroing free space
            node.serialize(buf.asIntBuffer());
            buf.flip();

            try {
                // Write this node as a page in file
                output.write(buf, currentPage * Database.PAGE_SIZE);
            } catch (IOException e) {
                e.printStackTrace();
            }

            currentIndex += entriesToWrite;
            entriesRemaining -= entriesToWrite;

            currentPage++;
        }
    }

}
