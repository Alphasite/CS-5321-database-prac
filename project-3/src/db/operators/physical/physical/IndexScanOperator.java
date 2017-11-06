package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.index.BTree;
import db.datastore.tuple.Tuple;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

public class IndexScanOperator extends AbstractOperator{

    private final BTree indexTree;
    private final Integer lowVal;
    private final Integer highVal;

    private BTree.BTreeDataIterator indexTreeIterator;

    /**
     * Note: colName is *just* the column name. Alias is not needed because this scans the base table.
     * @param indexTree The B+ tree that serves as the index for the table we are pulling tuples from
     * @param lowVal The minimum value (inclusive) tuples should contain for the key of indexTree
     * @param highVal The maximum value (inclusive) tuples should contain for the key of indexTree
     */
    public IndexScanOperator(BTree indexTree, Integer lowVal, Integer highVal) {
        this.indexTree = indexTree;
        this.lowVal = lowVal;
        this.highVal = highVal;

        this.indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);
    }

    @Override
    protected Tuple generateNextTuple() {
        return null;
    }

    @Override
    public TableHeader getHeader() {
        return null;
    }

    @Override
    public boolean reset() {
        indexTreeIterator = indexTree.iteratorForRange(lowVal, highVal);
        return true;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void close() {

    }
}
