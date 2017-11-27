package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.UnaryNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
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
        // Use LinkedHashSet to preserve insertion order
        Set<String> sortingAttributes = new LinkedHashSet<>(sortHeader.getQualifiedAttributeNames());
        Set<String> allAttributes = new LinkedHashSet<>(tupleLayout.getQualifiedAttributeNames());

        // Append extra attributes in order defined by tuple layout
        sortingAttributes.addAll(allAttributes);

        return new TableHeader(new ArrayList<>(sortingAttributes));
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
