package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;
import net.sf.jsqlparser.expression.Expression;

public class LogicalSelectOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private final Expression selectCondition;

    public LogicalSelectOperator(LogicalOperator source, Expression selectCondition) {
        this.source = source;
        this.selectCondition = selectCondition;
    }

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
