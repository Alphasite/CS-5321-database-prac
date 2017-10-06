package db.operators.logical;

import db.datastore.TableInfo;

public class LogicalScanOperator implements LogicalOperator {
    private final TableInfo table;

    public LogicalScanOperator(TableInfo table) {
        this.table = table;
    }
}
