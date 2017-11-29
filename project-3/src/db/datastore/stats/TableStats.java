package db.datastore.stats;

import db.datastore.TableInfo;

public class TableStats {
    protected TableInfo table;
    public int count;
    public int[] minimums;
    public int[] maximums;

    public TableStats(TableInfo table) {
        int numOfColumns = table.header.size();

        this.table = table;

        this.count = 0;
        this.minimums = new int[numOfColumns];
        this.maximums = new int[numOfColumns];

        for (int i = 0; i < numOfColumns; i++) {
            this.minimums[i] = Integer.MAX_VALUE;
            this.maximums[i] = Integer.MIN_VALUE;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(this.table.tableName);
        builder.append(' ');
        builder.append(count);

        for (int i = 0; i < this.table.header.columnNames.size(); i++) {
            builder.append(' ');
            builder.append(this.table.header.columnNames.get(i));
            builder.append(',');
            builder.append(this.minimums[i]);
            builder.append(',');
            builder.append(this.maximums[i]);
        }

        return builder.toString();
    }
}
