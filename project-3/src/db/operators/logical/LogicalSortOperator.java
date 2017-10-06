package db.operators.logical;

import db.datastore.TableHeader;

public class LogicalSortOperator {
    private final LogicalOperator source;
    private TableHeader sortHeader;

    public LogicalSortOperator(LogicalOperator source, TableHeader sortHeader) {
        this.source = source;
        this.sortHeader = sortHeader;
    }
}
