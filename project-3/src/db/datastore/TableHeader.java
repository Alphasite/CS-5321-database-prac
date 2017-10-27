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
    public final List<String> columnAliases;
    public final List<String> columnHeaders;

    /**
     * A no args constructor for sorting by no criteria.
     */
    public TableHeader() {
        this.columnAliases = new ArrayList<>();
        this.columnHeaders = new ArrayList<>();
    }

    /**
     * The table alias names and column names of the table.
     *
     * @param columnAlias   The name of the table from which the column originates.
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

    /**
     * Find the index of the alias, name combo.
     *
     * @param alias  The table name
     * @param column The column name
     * @return The index of the alias, name combo.
     */
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

    /**
     * @inheritDoc
     */
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

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader clone() {
        return new TableHeader(
                new ArrayList<>(this.columnAliases),
                new ArrayList<>(this.columnHeaders)
        );
    }
}
