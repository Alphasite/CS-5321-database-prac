package db.operators.logical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;

import java.util.ArrayList;

/**
 * An operator representing a scan.
 *
 * @inheritDoc
 */
public class LogicalScanOperator implements LogicalOperator {
    private final String tableName;
    private final TableInfo table;
    private final TableHeader header;

    /**
     * @param table The table info
     * @param tableName The name of the table.
     */
    public LogicalScanOperator(TableInfo table, String tableName) {
        this.table = table;
        this.tableName = tableName;
        this.header = computeHeader(table.header, tableName);
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
        return header;
    }

    /**
     * @return Get the name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Compute the result header of a rename.
     *
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
}
