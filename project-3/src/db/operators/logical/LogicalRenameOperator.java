package db.operators.logical;

public class LogicalRenameOperator {
    private final LogicalOperator child;
    private String newTableName;

    public LogicalRenameOperator(LogicalOperator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;
    }
}
