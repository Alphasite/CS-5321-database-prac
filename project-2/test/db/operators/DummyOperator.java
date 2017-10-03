package db.operators;

import db.datastore.TableHeader;
import db.datastore.Tuple;

import java.util.List;

public class DummyOperator implements Operator {

    private List<Tuple> tuples;
    private TableHeader header;
    private int i;

    public DummyOperator(List<Tuple> tuples, TableHeader header) {
        this.tuples = tuples;
        this.header = header;
        this.i = 0;
    }

    @Override
    public Tuple getNextTuple() {
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
}
