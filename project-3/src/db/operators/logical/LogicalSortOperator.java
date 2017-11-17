package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Logical operator for sorting : generates a header describing the order in which columns are used to sort tuples
 */
public class LogicalSortOperator implements LogicalOperator, UnaryNode<LogicalOperator> {
    private final LogicalOperator source;
    private TableHeader sortHeader;

    /**
     * @param source     the child operator
     * @param sortHeader the header which controls the sort order.
     */
    public LogicalSortOperator(LogicalOperator source, TableHeader sortHeader) {
        this.source = source;

        this.sortHeader = computeSortHeader(sortHeader, source.getHeader());
    }

    /**
     * Compute the full sorting header for this sort from partial sort header and tuple layout
     */
    public static TableHeader computeSortHeader(TableHeader sortHeader, TableHeader tupleLayout) {
        Set<String> alreadySortedColumns = new HashSet<>();

        List<String> aliases = new ArrayList<>();
        List<String> columns = new ArrayList<>();

        for (int i = 0; i < sortHeader.tableIdentifiers.size(); i++) {
            String alias = sortHeader.tableIdentifiers.get(i);
            String column = sortHeader.columnNames.get(i);
            String fullName = alias + "." + column;

            if (!alreadySortedColumns.contains(fullName)) {
                aliases.add(alias);
                columns.add(column);
                alreadySortedColumns.add(fullName);
            }
        }

        for (int i = 0; i < tupleLayout.tableIdentifiers.size(); i++) {
            String alias = tupleLayout.tableIdentifiers.get(i);
            String column = tupleLayout.columnNames.get(i);
            String fullName = alias + "." + column;

            // Append non specified columns so that they are used to break ties
            if (!alreadySortedColumns.contains(fullName)) {
                aliases.add(alias);
                columns.add(column);
                alreadySortedColumns.add(fullName);
            }
        }

        return new TableHeader(aliases, columns);
    }

    /**
     * The header which indicates the sort priorities.
     *
     * @return The header
     */
    public TableHeader getSortHeader() {
        return sortHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public LogicalOperator getChild() {
        return source;
    }
}
