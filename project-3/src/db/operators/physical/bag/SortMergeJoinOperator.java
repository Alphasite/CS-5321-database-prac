package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import net.sf.jsqlparser.expression.Expression;

public class SortMergeJoinOperator implements JoinOperator {
    @Override
    public Expression getPredicate() {
        return null;
    }

    @Override
    public Operator getLeft() {
        return null;
    }

    @Override
    public Operator getRight() {
        return null;
    }

    @Override
    public Tuple getNextTuple() {
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
    public boolean reset(int index) {
        return false;
    }

    @Override
    public int getTupleIndex() {
        return 0;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {

    }

    @Override
    public void close() {
        
    }
}
