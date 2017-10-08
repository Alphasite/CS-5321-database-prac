package db.operators.logical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;

public class LogicalScanOperator implements LogicalOperator {

    private final TableInfo table;

    public LogicalScanOperator(TableInfo table) {
        this.table = table;
    }

    public TableInfo getTable() {
        return table;
    }

    @Override
    public TableHeader getHeader() {
        return table.header;
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }
}
