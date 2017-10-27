package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

/**
 * The logical operator which represents a projection operator.
 *
 * @inheritDoc
 */
public class LogicalProjectOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private final TableHeader newHeader;

    /**
     * @param source    the source header
     * @param newHeader the new header for result tuples
     */
    public LogicalProjectOperator(LogicalOperator source, TableHeader newHeader) {
        this.source = source;
        this.newHeader = newHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return newHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public LogicalOperator getChild() {
        return source;
    }
}
