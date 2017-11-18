package db.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An object which contains the mappings required to resolve a column index from its fully qualified name.
 * For example:
 * boat.name,
 * name,
 * etc.
 */
public class TableHeader {
    public final List<String> tableIdentifiers;
    public final List<String> columnNames;

    /**
     * A no args constructor for sorting by no criteria.
     */
    public TableHeader() {
        this.tableIdentifiers = new ArrayList<>();
        this.columnNames = new ArrayList<>();
    }

    /**
     * The table alias names and column names of the table.
     *
     * @param tableNames  The identifier of the table containing the attribute.
     * @param columnNames The name of the column.
     */
    public TableHeader(List<String> tableNames, List<String> columnNames) {
        this.tableIdentifiers = tableNames;
        this.columnNames = columnNames;
    }

    /**
     * @return The width of the table.
     */
    public int size() {
        return this.columnNames.size();
    }

    /**
     * Find the index of the alias, name combo.
     *
     * @param table  The table name
     * @param column The column name
     * @return The index of the alias, name combo.
     */
    public Optional<Integer> resolve(String table, String column) {
        boolean notRequireAliasMatch = table.equals("");

        for (int i = 0; i < columnNames.size(); i++) {
            boolean aliasMatch = tableIdentifiers.get(i).equals(table);
            boolean headerMatch = columnNames.get(i).equals(column);

            if ((notRequireAliasMatch || aliasMatch) && headerMatch) {
                return Optional.of(i);
            }
        }

        return Optional.empty();
    }

    /**
     * @inheritDoc
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append(this.tableIdentifiers.get(0));
        builder.append(".");
        builder.append(this.columnNames.get(0));

        for (int i = 1; i < this.columnNames.size(); i++) {
            builder.append(" | ");
            builder.append(this.tableIdentifiers.get(i));
            builder.append(".");
            builder.append(this.columnNames.get(i));
        }

        return builder.toString();
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader clone() {
        return new TableHeader(
                new ArrayList<>(this.tableIdentifiers),
                new ArrayList<>(this.columnNames)
        );
    }
}
