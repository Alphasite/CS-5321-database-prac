package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

public class LogicalDistinctOperator implements LogicalOperator, UnaryNode<LogicalOperator> {

    private LogicalOperator source;

    public LogicalDistinctOperator(LogicalOperator source) {
        this.source = source;
    }

    @Override
    public LogicalOperator getChild() {
        return source;
    }

    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }
}
