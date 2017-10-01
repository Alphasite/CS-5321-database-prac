package operators.extended;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.Objects;

/** This operator removes any duplicate tuples from the input.
 *
 * The implementation is such that the source tuple must provide tuples in a sorted order for proper function.
 *
 * @inheritDoc
 */
public class DistinctOperator implements Operator {
    private Operator source;
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
        //TODO:
        while (true) {
            Tuple next = this.source.getNextTuple();
            if (next == null) {
                return null;
            } else {
                if (!Objects.equals(this.previous, next)) {
                    this.previous = next;
                    return next;
                }
            }
        }
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
