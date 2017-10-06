package db.operators.logical;

import db.datastore.TableHeader;

import java.util.List;

public class LogicalProjectOperator implements LogicalOperator {
    private final LogicalOperator source;
    private final TableHeader newHeader;
    private final List<Integer> newToOldColumnMapping;

    public LogicalProjectOperator(LogicalOperator source, TableHeader newHeader, List<Integer> newToOldColumnMapping) {
        this.source = source;
        this.newHeader = newHeader;
        this.newToOldColumnMapping = newToOldColumnMapping;
    }
}
