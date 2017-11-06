package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.datastore.tuple.Tuple;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

public class IndexScanOperator extends AbstractOperator{

    private final BTree indexTree;
    private final Integer lowVal;
    private final Integer highVal;


    /**
     * Note: colName is *just* the column name. Alias is not needed because this scans the base table.
     * @param source
     * @param colName
     */
    public IndexScanOperator(BTree indexTree, String colName, Integer lowVal, Integer highVal) {
        this.indexTree = indexTree;
        this.lowVal = lowVal;
        this.highVal = highVal;

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
        return false;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {

    }

    @Override
    public void close() {

    }
}
