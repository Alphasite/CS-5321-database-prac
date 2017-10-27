package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;
import net.sf.jsqlparser.expression.Expression;

/**
 * An operator representing a select operation.
 *
 * @inheritDoc
 */
public class LogicalSelectOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private final Expression selectCondition;

    /**
     * @param source          the child operator
     * @param selectCondition the condition which controls which tuples can pass.
     */
    public LogicalSelectOperator(LogicalOperator source, Expression selectCondition) {
        this.source = source;
        this.selectCondition = selectCondition;
    }

    /**
     * @return the filter predicate.
     */
    public Expression getPredicate() {
        return selectCondition;
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

    /**
     * @inheritDoc
     */
    @Override
    public LogicalOperator getChild() {
        return source;
    }
}
