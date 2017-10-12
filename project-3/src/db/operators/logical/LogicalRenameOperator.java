package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

import java.util.ArrayList;

public class LogicalRenameOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator child;
    private String newTableName;

    private final TableHeader outputHeader;

    public LogicalRenameOperator(LogicalOperator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;
        this.outputHeader = computeHeader(child.getHeader(), newTableName);
    }

    public static TableHeader computeHeader(TableHeader sourceHeader, String newTableName) {
        ArrayList<String> newAliases = new ArrayList<>();
        for (int i = 0; i < sourceHeader.size(); i++) {
            newAliases.add(newTableName);
        }

        return new TableHeader(newAliases, sourceHeader.columnHeaders);
    }

    public String getNewTableName() {
        return newTableName;
    }

    @Override
    public TableHeader getHeader() {
        return outputHeader;
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public LogicalOperator getChild() {
        return child;
    }
}
