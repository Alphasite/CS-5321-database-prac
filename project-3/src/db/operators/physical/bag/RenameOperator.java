package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalRenameOperator;
import db.operators.physical.Operator;

/**
 * Rename the table of the child tuples' schema.
 * <p>
 * This operator changes the alias field of the header, so that all parent operator nodes
 * can reference the new alias instead of the original name.
 * This assumes that the queries do not use the original name once an alias has been defined.
 *
 * @inheritDoc
 */
public class RenameOperator implements Operator {
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

        this.header = LogicalRenameOperator.computeHeader(child, newTableName);
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
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
}
