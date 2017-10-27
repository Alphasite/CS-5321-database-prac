package db.operators.physical;

import db.datastore.tuple.Tuple;

/**
 * An operator class which provides default implementations of several simple methods:
 *
 * @inheritDoc
 */
public abstract class AbstractOperator implements Operator {
    protected Tuple next;

    /**
     * Create a new operator with no cached tuples.
     */
    public AbstractOperator() {
        this.next = null;
    }

    /**
     * Compute a tuple from the underlying operator/file
     *
     * @return a tuple from the child
     */
    protected abstract Tuple generateNextTuple();

    /**
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        Tuple next = this.peekNextTuple();
        this.next = null;
        return next;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple peekNextTuple() {
        if (this.next == null) {
            this.next = this.generateNextTuple();
        }

        return this.next;
    }
}
