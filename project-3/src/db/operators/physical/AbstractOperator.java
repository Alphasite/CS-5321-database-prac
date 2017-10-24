package db.operators.physical;

import db.datastore.tuple.Tuple;

public abstract class AbstractOperator implements Operator {
    protected Tuple next;

    public AbstractOperator() {
        this.next = null;
    }

    protected abstract Tuple generateNextTuple();

    @Override
    public Tuple getNextTuple() {
        Tuple next = this.peekNextTuple();
        this.next = null;
        return next;
    }

    @Override
    public Tuple peekNextTuple() {
        if (this.next == null) {
            this.next = this.generateNextTuple();
        }

        return this.next;
    }
}
