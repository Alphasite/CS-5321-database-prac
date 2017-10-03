package db.datastore;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An object which contains the mappings required to resolve a column index from its fully qualified name.
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
     * Creates a TableHeader from a list of Column objects containing info about a column and its source table
     */
    public static TableHeader fromColumns(List<Column> columns) {
        List<String> tableNames = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        for (Column c : columns) {
            tableNames.add(c.getTable().getName());
            columnNames.add(c.getColumnName());
        }

        return new TableHeader(tableNames, columnNames);
    }

    /**
     * @return The width of the table.
     */
    public int size() {
        return this.columnHeaders.size();
    }

    public Optional<Integer> resolve(String alias, String column) {
        boolean notRequireAliasMatch = alias.equals("");

        for (int i = 0; i < columnHeaders.size(); i++) {
            boolean aliasMatch = columnAliases.get(i).equals(alias);
            boolean headerMatch = columnHeaders.get(i).equals(column);

            if ((notRequireAliasMatch || aliasMatch) && headerMatch) {
                return Optional.of(i);
            }
        }

        return Optional.empty();
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
