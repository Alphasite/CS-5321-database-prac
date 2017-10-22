package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.logical.LogicalRenameOperator;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

/**
 * Rename the table of the child tuples' schema.
 * <p>
 * This operator changes the alias field of the header, so that all parent operator nodes
 * can reference the new alias instead of the original name.
 * This assumes that the queries do not use the original name once an alias has been defined.
 *
 * @inheritDoc
 */
public class RenameOperator extends AbstractOperator implements UnaryNode<Operator> {
    private final Operator child;

    private String newTableName;
    private TableHeader header;


    /**
     * @param child        The child operator.
     * @param newTableName The new name of the table.
     */
    public RenameOperator(Operator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;

        this.header = LogicalRenameOperator.computeHeader(child.getHeader(), newTableName);
    }

    /**
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        return this.child.getNextTuple();
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return header;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        return this.child.reset();
    }

    @Override
    public boolean reset(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTupleIndex() {
        throw new UnsupportedOperationException();
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
        this.child.close();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return child;
    }

    /**
     * @return The new name of the table.
     */
    public String getNewTableName() {
        return newTableName;
    }
}
