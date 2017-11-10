package db.datastore.stats;

import db.datastore.TableInfo;

public class TableStats {
    protected TableInfo table;
    protected int count;
    protected int[] minimums;
    protected int[] maximums;

    public TableStats(TableInfo table) {
        int numOfColumns = table.header.size();

        this.table = table;

        this.count = 0;
        this.minimums = new int[numOfColumns];
        this.maximums = new int[numOfColumns];
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(this.table.tableName);
        builder.append(' ');
        builder.append(count);

        for (int i = 0; i < this.table.header.columnHeaders.size(); i++) {
            builder.append(' ');
            builder.append(this.table.header.columnHeaders.get(i));
            builder.append(',');
            builder.append(this.minimums[i]);
            builder.append(',');
            builder.append(this.maximums[i]);
        }

        return builder.toString();
    }
}
