package db.operators.logical;

import db.datastore.TableHeader;

public class LogicalProjectOperator implements LogicalOperator {
    private final LogicalOperator source;
    private final TableHeader newHeader;

    public LogicalProjectOperator(LogicalOperator source, TableHeader newHeader) {
        this.source = source;
        this.newHeader = newHeader;
    }
}
