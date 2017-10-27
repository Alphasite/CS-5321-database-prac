package db.operators.logical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;

/**
 * An operator representing a scan.
 *
 * @inheritDoc
 */
public class LogicalScanOperator implements LogicalOperator {

    private final TableInfo table;

    /**
     * @param table the table to be scanned.
     */
    public LogicalScanOperator(TableInfo table) {
        this.table = table;
    }

    /**
     * @return the table which is scanned by this relation
     */
    public TableInfo getTable() {
        return table;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return table.header;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }
}
