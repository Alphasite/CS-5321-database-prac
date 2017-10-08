package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

public class LogicalProjectOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private final TableHeader newHeader;

    public LogicalProjectOperator(LogicalOperator source, TableHeader newHeader) {
        this.source = source;
        this.newHeader = newHeader;
    }

    @Override
    public TableHeader getHeader() {
        return newHeader;
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public LogicalOperator getChild() {
        return source;
    }
}
