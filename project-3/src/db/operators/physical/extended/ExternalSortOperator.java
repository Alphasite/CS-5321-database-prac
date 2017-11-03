package db.operators.physical.extended;

import db.Utilities;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.SeekableOperator;
import db.operators.physical.utility.BlockCacheOperator;
import db.operators.physical.utility.ExternalBlockCacheOperator;
import db.performance.DiskIOStatistics;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A merge-sort based sorting operator implementation that guarantees bounded state by only keeping a specified
 * number of pages in memory. Merge passes are saved as temporary files, direct access is abstracted via Cache classes.
 */
public class ExternalSortOperator extends AbstractOperator implements SortOperator, UnaryNode<Operator>, SeekableOperator {
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

    /**
     * When sorting has completed, data can be retrieved from disk through this cache
     */
    private ExternalBlockCacheOperator sortedRelationCache;

    private int tupleIndex;

    /**
     * Configure a new operator to handle External sorting. Sorting is only performed when the first tuple is requested
     *
     * @param source Operator to read tuples from
     * @param sortHeader Defines against which attributes the relation will be sorted (no tie-break)
     * @param bufferSize Number of buffer pages held in memory. Must be >= 3
     * @param tempFolder Folder to write temporary merged runs to
     */
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

        this.tupleIndex = -1;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        // No need to sort data again, just reset reader if present
        if (isSorted && sortedRelationCache != null) {
            return sortedRelationCache.reset();
        } else {
            return false;
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public int getTupleIndex() {
        return tupleIndex;
    }

    /**
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        if (!isSorted) {
            System.out.println("Beginning sort");
            performExternalSort();
            System.out.println("Complete !");
        }

        this.tupleIndex++;
        return sortedRelationCache.getNextTuple();
    }

    /**
     * Sort and buffer the input tuples from the child relations, using the external sort algorithm.
     */
    private void performExternalSort() {
        // First pass : read pages from source, sort then in-memory and write the resulting run to disk
        List<ExternalBlockCacheOperator> previousRuns = new ArrayList<>();
        int blockId = 1;

        // Create a cache that reads tuples from source one page at a time
        BlockCacheOperator inputCache = new BlockCacheOperator(source, Database.PAGE_SIZE * bufSize);

        System.out.println("Pass 1");
        System.out.println("Opened: " + DiskIOStatistics.handles_opened);
        System.out.println("Closed: " + DiskIOStatistics.handles_closed);

        while (inputCache.hasNext()) {
            System.out.println("Sorting " + blockId);
            System.out.println("Opened: " + DiskIOStatistics.handles_opened);
            System.out.println("Closed: " + DiskIOStatistics.handles_closed);

            InMemorySortOperator inMemorySort = new InMemorySortOperator(inputCache, sortHeader);
            ExternalBlockCacheOperator tempRun = new ExternalBlockCacheOperator(getHeader(), sortFolder, "Sort" + operatorId + "_1_" + blockId);

            System.out.println("Opened: " + DiskIOStatistics.handles_opened);
            System.out.println("Closed: " + DiskIOStatistics.handles_closed);

            tempRun.writeSourceToBuffer(inMemorySort);

            System.out.println("Opened: " + DiskIOStatistics.handles_opened);
            System.out.println("Closed: " + DiskIOStatistics.handles_closed);

            tempRun.flush();
            previousRuns.add(tempRun);

            System.out.println("Opened: " + DiskIOStatistics.handles_opened);
            System.out.println("Closed: " + DiskIOStatistics.handles_closed);

            inputCache.loadNextBlock();
            blockId++;

            System.out.println("Opened: " + DiskIOStatistics.handles_opened);
            System.out.println("Closed: " + DiskIOStatistics.handles_closed);
        }

        // This releases all resources held by source operator
        inputCache.close();

        // Second to last pass : merge previous runs using a fixed size buffer
        int runId = 2;
        blockId = 1;

        System.out.println("Pass 2");

        System.out.println("Opened: " + DiskIOStatistics.handles_opened);
        System.out.println("Closed: " + DiskIOStatistics.handles_closed);

        while (previousRuns.size() >= 2) {
            List<ExternalBlockCacheOperator> currentRuns = new ArrayList<>();

            // Load N - 1 input files into memory with N = buffer size
            List<Operator> currentMergeInputs = new ArrayList<>();
            for (int i = 1; i <= previousRuns.size(); i++) {
                Operator mergeInput = previousRuns.get(i - 1);
                currentMergeInputs.add(mergeInput);

                if (i % (bufSize - 1) == 0 || i == previousRuns.size()) {
                    // All necessary pages are loaded : run merge pass
                    ExternalBlockCacheOperator mergeCache = new ExternalBlockCacheOperator(
                            getHeader(),
                            sortFolder,
                            "Sort" + operatorId + "_" + runId + "_" + blockId
                    );

                    currentRuns.add(mergeCache);

                    System.out.println("Merging " + i);

                    System.out.println("Opened: " + DiskIOStatistics.handles_opened);
                    System.out.println("Closed: " + DiskIOStatistics.handles_closed);

                    performMultiMerge(currentMergeInputs, mergeCache);

                    mergeCache.flush();

                    for (Operator op : currentMergeInputs) {
                        op.close();
                    }

                    currentMergeInputs.clear();

                    System.out.println("Merge done");

                    blockId++;

                    System.out.println("Opened: " + DiskIOStatistics.handles_opened);
                    System.out.println("Closed: " + DiskIOStatistics.handles_closed);
                }
            }

            previousRuns = currentRuns;
            runId++;
        }

        this.isSorted = true;

        this.sortedRelationCache = previousRuns.get(0);

        System.out.println("Opened: " + DiskIOStatistics.handles_opened);
        System.out.println("Closed: " + DiskIOStatistics.handles_closed);
    }

    /**
     * Merge the input runs and write the results to the output run.
     *
     * @param inputs the input runs to merge.
     * @param output the run which will contain the output tuples.
     */
    private void performMultiMerge(List<Operator> inputs, ExternalBlockCacheOperator output) {
        List<Tuple> inputTuples = new ArrayList<>(inputs.size());
        int inputRemaining = inputs.size();

        // Load first tuple from every source
        for (Operator source : inputs) {
            Tuple next = source.getNextTuple();
            inputTuples.add(next);

            if (next == null) {
                inputRemaining--;
            }
        }

        // Perform merging
        while (inputRemaining > 0) {
            int iMin = Utilities.getFirstTuple(inputTuples, tupleComparator);

            output.writeTupleToBuffer(inputTuples.get(iMin));
            Tuple next = inputs.get(iMin).getNextTuple();
            inputTuples.set(iMin, next);

            if (next == null) {
                inputRemaining--;
            }
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return source;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getSortHeader() {
        return sortHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.source.close();

        if (this.sortedRelationCache != null) {
            this.sortedRelationCache.close();
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void seek(int index) {
        this.sortedRelationCache.seek(index);
        this.next = null;
        this.tupleIndex = index-1;
    }
}
