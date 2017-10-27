package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

/**
 * A logical node which represents the distinct operation.
 *
 * @inheritDoc
 */
public class LogicalDistinctOperator implements LogicalOperator, UnaryNode<LogicalOperator> {

    private LogicalOperator source;

    public LogicalDistinctOperator(LogicalOperator source) {
        this.source = source;
    }

    /**
     * @inheritDoc
     */
    @Override
    public LogicalOperator getChild() {
        return source;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }
}
