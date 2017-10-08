package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

public class LogicalSortOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private TableHeader sortHeader;

    public LogicalSortOperator(LogicalOperator source, TableHeader sortHeader) {
        this.source = source;
        this.sortHeader = sortHeader;
    }

    public TableHeader getSortHeader() {
        return sortHeader;
    }

    @Override
    public TableHeader getHeader() {
        return source.getHeader();
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
