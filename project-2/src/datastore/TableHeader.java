package datastore;

import java.util.ArrayList;
import java.util.List;

/**
 * An object which contains the mappings required to resolve a comlumn index from its fully qualified name.
 * For example:
 *  boat.name,
 *  name,
 *  etc.
 */
public class TableHeader {
    public final List<String> columnAliases;
    public final List<String> columnHeaders;


    /** The table alias names and column names of the table.
     * @param columnAlias The name of the table from which the column originates.
     * @param columnHeaders The name of the column.
     */
    public TableHeader(List<String> columnAlias, List<String> columnHeaders) {
        this.columnAliases = columnAlias;
        this.columnHeaders = columnHeaders;
    }


    /**
     * @return The width of the table.
     */
    public int size() {
        return this.columnHeaders.size();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(this.columnAliases.get(0));
        builder.append(".");
        builder.append(this.columnHeaders.get(0));

        for (int i = 1; i < this.columnHeaders.size(); i++) {
            builder.append(" | ");
            builder.append(this.columnAliases.get(i));
            builder.append(".");
            builder.append(this.columnHeaders.get(i));
        }

        return builder.toString();
    }

    @Override
    public TableHeader clone() {
        return new TableHeader(
                new ArrayList<>(this.columnAliases),
                new ArrayList<>(this.columnHeaders)
        );
    }
}
