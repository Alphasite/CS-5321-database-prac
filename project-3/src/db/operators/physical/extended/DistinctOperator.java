package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.Objects;

/**
 * This operator removes any duplicate tuples from the input.
 * <p>
 * The implementation is such that the source tuple must provide tuples in a sorted order for proper function.
 *
 * @inheritDoc
 */
public class DistinctOperator extends AbstractOperator implements UnaryNode<Operator> {
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
    protected Tuple generateNextTuple() {
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

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.source.close();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return source;
    }
}
