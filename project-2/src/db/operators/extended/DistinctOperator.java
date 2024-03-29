package db.operators.extended;

import db.datastore.TableHeader;
import db.datastore.Tuple;
import db.operators.Operator;

import java.util.Objects;

/** This operator removes any duplicate tuples from the input.
 *
 * The implementation is such that the source tuple must provide tuples in a sorted order for proper function.
 *
 * @inheritDoc
 */
public class DistinctOperator implements Operator {
    private final Operator source;

    private Tuple previous;

    /**
     * @param source The child node which provides tuples in a sorted order.
     */
    public DistinctOperator(Operator source) {
        this.source = source;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        Tuple next;

        while ((next = this.source.getNextTuple()) != null) {
            if (!Objects.equals(this.previous, next)) {
                this.previous = next;
                return next;
            }
        }

        return null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        return this.source.reset();
    }

}
