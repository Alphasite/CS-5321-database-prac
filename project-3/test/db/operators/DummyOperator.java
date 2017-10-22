package db.operators;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.List;

public class DummyOperator extends AbstractOperator {

    private List<Tuple> tuples;
    private TableHeader header;
    private int i;

    public DummyOperator(List<Tuple> tuples, TableHeader header) {
        this.tuples = tuples;
        this.header = header;
        this.i = 0;
    }

    @Override
    protected Tuple generateNextTuple() {
        return i < tuples.size() ? tuples.get(i++) : null;
    }

    @Override
    public TableHeader getHeader() {
        return this.header;
    }

    @Override
    public boolean reset() {
        this.i = 0;
        return true;
    }

    @Override
    public boolean reset(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTupleIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        // Not used for the mock class.
    }

    @Override
    public void close() {
        // Not used for the mock class.
    }
}
