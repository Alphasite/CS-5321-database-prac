package db.operators.physical.physical;

import db.datastore.IndexInfo;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.datastore.index.Rid;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.operators.logical.LogicalScanOperator;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

/**
 * An operator that uses an index on a table to scan through
 * a specific range of tuples in that table.
 */
public class IndexScanOperator extends AbstractOperator {

    private final TableInfo tableInfo;
    private final BTree indexTree;
    private final Integer lowVal;
    private final Integer highVal;
    private final IndexInfo index;
    private final TableHeader header;

    private BinaryTupleReader reader;
    private BTree.BTreeDataIterator indexTreeIterator;

    /**
     * @param tableInfo The table that this operator is scanning
     * @param indexTree The B+ tree that serves as the index for the table we are pulling tuples from
     * @param lowVal The minimum value (inclusive) tuples should contain for the key of indexTree
     * @param highVal The maximum value (inclusive) tuples should contain for the key of indexTree
     */
    public IndexScanOperator(TableInfo tableInfo, String tableAlias, IndexInfo index, BTree indexTree, Integer lowVal, Integer highVal) {
        this.tableInfo = tableInfo;
        this.indexTree = indexTree;
        this.lowVal = lowVal;
        this.highVal = highVal;
        this.indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);
        this.index = index;
        this.header = LogicalScanOperator.computeHeader(tableInfo.header, tableAlias);

        reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        if (this.index.isClustered) {
            return generateNextTupleClustered();
        } else {
            return generateNextTupleUnclustered();
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.header;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        if (this.index.isClustered) {
            return resetClustered();
        } else {
            return resetUnclustered();
        }
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
    public void close() {
        if (this.reader != null) {
            this.reader.close();
        }
    }

    /**
     * @return the underlying table info instance.
     */
    public TableInfo getTable() {
        return tableInfo;
    }

    /**
     * Generates the next tuple when using a clustered index.
     *
     * @return the next tuple
     */
    private Tuple generateNextTupleClustered() {
        int index = this.tableInfo.header.resolve(this.tableInfo.tableName, this.index.attributeName).get();
        boolean boundedByLow = (lowVal == null) || (this.reader.peek().fields.get(index) >= lowVal);
        boolean boundedByHigh = (highVal == null) || (this.reader.peek().fields.get(index) <= highVal);
        if (boundedByLow && boundedByHigh) {
            return this.reader.next();
        } else {
            return null;
        }
    }

    /**
     * Generates the next tuple when using an unclustered index.
     *
     * @return the next tuple
     */
    private Tuple generateNextTupleUnclustered() {
        if (indexTreeIterator.hasNext()) {
            Rid r = indexTreeIterator.next();
            this.reader.seek(r.pageid, r.tupleid);
            return this.reader.next();
        } else {
            return null;
        }
    }

    /**
     * Resets this operator when using a clustered tree
     *
     * @return true in all cases
     */
    private boolean resetClustered() {
        resetUnclustered();
        if (indexTreeIterator.hasNext()) {
            Rid r = indexTreeIterator.next();
            this.reader.seek(r.pageid, r.tupleid);
        }
        return true;
    }

    /**
     * Resets this operator when using an unclustered tree
     *
     * @return true in all cases
     */
    private boolean resetUnclustered() {
        close();
        this.reader = BinaryTupleReader.get(this.tableInfo.file);
        indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);
        return true;
    }

    /**
     * @return the lower bound for the index scan
     */
    public Integer getLowVal() {
        return lowVal;
    }

    /**
     * @return the upper bound for the index scan
     */
    public Integer getHighVal() {
        return highVal;
    }

    /**
     * @return the index which is being used
     */
    public IndexInfo getIndex() {
        return index;
    }
}
