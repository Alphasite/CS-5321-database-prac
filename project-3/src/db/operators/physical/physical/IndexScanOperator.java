package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.datastore.index.Rid;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

public class IndexScanOperator extends AbstractOperator{

    private final TableInfo tableInfo;
    private final BTree indexTree;
    private final Integer lowVal;
    private final Integer highVal;

    private BinaryTupleReader reader;
    private BTree.BTreeDataIterator indexTreeIterator;

    /**
     * @param tableInfo The table that this operator is scanning
     * @param indexTree The B+ tree that serves as the index for the table we are pulling tuples from
     * @param lowVal The minimum value (inclusive) tuples should contain for the key of indexTree
     * @param highVal The maximum value (inclusive) tuples should contain for the key of indexTree
     */
    public IndexScanOperator(TableInfo tableInfo, BTree indexTree, Integer lowVal, Integer highVal) {
        this.tableInfo = tableInfo;
        this.indexTree = indexTree;
        this.lowVal = lowVal;
        this.highVal = highVal;
        this.indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);

        reset();
    }

    @Override
    protected Tuple generateNextTuple() {
        if (this.tableInfo.index.isClustered) {
            return generateNextTupleClustered();
        } else {
            return generateNextTupleUnclustered();
        }
    }
    @Override
    public TableHeader getHeader() {
        return tableInfo.header;
    }

    @Override
    public boolean reset() {
        if (this.tableInfo.index.isClustered) {
            return resetClustered();
        } else {
            return resetUnclustered();
        }
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

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

    private Tuple generateNextTupleClustered() {
        return this.reader.next();
    }

    private Tuple generateNextTupleUnclustered() {
        if (indexTreeIterator.hasNext()) {
            Rid r = indexTreeIterator.next();
            this.reader.seek(r.pageid, r.tupleid);
            return this.reader.next();
        } else {
            return null;
        }
    }

    private boolean resetClustered() {
        resetUnclustered();
        Rid r = indexTreeIterator.next();
        this.reader.seek(r.pageid, r.tupleid);
        return true;
    }

    private boolean resetUnclustered() {
        close();
        this.reader = BinaryTupleReader.get(this.tableInfo.file);
        indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);
        return true;
    }

}
