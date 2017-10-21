package db.operators.physical.extended;

import db.Utilities;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.datastore.tuple.string.StringTupleReader;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.UnaryNode;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.physical.BlockCacheOperator;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ExternalSortOperator implements Operator, UnaryNode<Operator> {
    private static int nextOperatorId = 1;

    private final Operator source;
    private final TableHeader sortHeader;
    private final Comparator<Tuple> tupleComparator;

    private final Path sortFolder;
    /** The number of pages held in memory during sorting and merging operations */
    private final int bufSize;

    private boolean isSorted;
    /** Temporary merge sort pages follow the nomenclature 'Sort<opId>_<runId>_<blockId>' */
    private int operatorId;

    private Path sortedRelationFile;
    private TupleReader sortedRelationReader;

    private boolean STRING_OUTPUT = true;

    public ExternalSortOperator(Operator source, TableHeader sortHeader, int bufferSize, Path tempFolder) {
        this.source = source;
        this.sortHeader = sortHeader;

        this.tupleComparator = new TupleComparator(sortHeader, source.getHeader());

        // Cannot perform merge sort with less than three buffer pages
        assert bufferSize >= 3;

        this.bufSize = bufferSize;
        this.sortFolder = tempFolder;

        this.isSorted = false;

        // Assign a new ID to keep temporary files separate from other instances
        this.operatorId = nextOperatorId++;
    }

    @Override
    public boolean reset() {
        // No need to sort data again, just reset reader if present
        if (isSorted && sortedRelationFile != null) {
            this.sortedRelationReader = getReader(getHeader(), sortedRelationFile.toFile());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tuple getNextTuple() {
        if (!isSorted) {
            performExternalSort();
        }

        // Check that we have a valid file to read from
        if (sortedRelationReader == null) {
            this.sortedRelationReader = getReader(getHeader(), sortedRelationFile.toFile());
        }

        return sortedRelationReader.next();
    }

    private TupleReader getReader(TableHeader header, File file) {
        if (STRING_OUTPUT)
            return StringTupleReader.get(header, file.toPath());
        else
            return BinaryTupleReader.get(file.toPath());
    }

    private TupleWriter getWriter(TableHeader header, File file) {
        if (STRING_OUTPUT)
            return StringTupleWriter.get(file);
        else
            return BinaryTupleWriter.get(header, file);
    }

    private void performExternalSort() {
        // First pass : read pages from source, sort then in-memory and write the resulting run to disk
        List<File> previousRunFiles = new ArrayList<>();
        int blockId = 1;

        // Create a cache that reads tuples from source one page at a time
        BlockCacheOperator cache = new BlockCacheOperator(source, Database.PAGE_SIZE * bufSize);

        while (cache.hasNext()) {
            SortOperator inMemorySort = new SortOperator(cache, sortHeader);
            File sortedPageFile = sortFolder.resolve("Sort" + operatorId + "_1_" + blockId).toFile();
            TupleWriter writer = getWriter(getHeader(), sortedPageFile);

            previousRunFiles.add(sortedPageFile);

            inMemorySort.dump(writer);
            writer.close();

            cache.loadNextBlock();
            blockId++;
        }

        // Second to last pass : merge previous runs using a fixed size buffer
        int runId = 2;
        blockId = 1;

        while (previousRunFiles.size() >= 2) {
            List<File> currentRunFiles = new ArrayList<>();

            // Load N - 1 input files into memory with N = buffer size
            List<TupleReader> currentRunReaders = new ArrayList<>();
            for (int i = 1; i <= previousRunFiles.size(); i++) {
                TupleReader reader = getReader(getHeader(), previousRunFiles.get(i - 1));
                currentRunReaders.add(reader);

                if (i % (bufSize - 1) == 0 || i == previousRunFiles.size()) {
                    // All necessary pages are loaded : run merge pass
                    File mergeOutputFile = sortFolder.resolve("Sort" + operatorId + "_" + runId + "_" + blockId).toFile();
                    currentRunFiles.add(mergeOutputFile);

                    performMultiMerge(currentRunReaders, getWriter(getHeader(), mergeOutputFile));
                    currentRunReaders.clear();

                    blockId++;
                }
            }

            previousRunFiles = currentRunFiles;
            runId++;
        }

        this.isSorted = true;

        this.sortedRelationFile = previousRunFiles.get(0).toPath();
        this.sortedRelationReader = getReader(getHeader(), sortedRelationFile.toFile());
    }

    private void performMultiMerge(List<TupleReader> readers, TupleWriter output) {
        List<Tuple> inputTuples = new ArrayList<>(readers.size());
        int inputRemaining = readers.size();

        // Load first tuple from every reader
        for (TupleReader reader : readers) {
            Tuple next = reader.next();
            inputTuples.add(next);

            if (next == null) {
                inputRemaining--;
            }
        }

        // Perform merging
        while (inputRemaining > 0) {
            int iMin = Utilities.getFirstTuple(inputTuples, tupleComparator);

            output.write(inputTuples.get(iMin));
            Tuple next = readers.get(iMin).next();
            inputTuples.set(iMin, next);

            if (next == null) {
                inputRemaining--;
            }
        }

        output.flush();
        output.close();
    }

    @Override
    public Operator getChild() {
        return source;
    }

    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
//        visitor.visit(this);
    }
}
