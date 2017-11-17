package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

import java.util.ArrayList;

/**
 * An operator representing a table rename.
 *
 * @inheritDoc
 */
public class LogicalRenameOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator child;
    private String newTableName;

    private final TableHeader outputHeader;

    /**
     * @param child        the node which has its tuples renamed.
     * @param newTableName the new name of the tuple.
     */
    public LogicalRenameOperator(LogicalOperator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;
        this.outputHeader = computeHeader(child.getHeader(), newTableName);
    }

    /**
     * Compute the result header of a rename.
     * @param sourceHeader the original header.
     * @param newTableName the new table name.
     * @return the result header.
     */
    public static TableHeader computeHeader(TableHeader sourceHeader, String newTableName) {
        ArrayList<String> newAliases = new ArrayList<>();
        for (int i = 0; i < sourceHeader.size(); i++) {
            newAliases.add(newTableName);
        }

        return new TableHeader(newAliases, sourceHeader.columnNames);
    }

    /**
     * @return the new name of the relation.
     */
    public String getNewTableName() {
        return newTableName;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return outputHeader;
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
        return child;
    }
}
