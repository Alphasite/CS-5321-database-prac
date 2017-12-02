package db.operators.logical;

import db.Utilities.Pair;
import db.Utilities.UnionFind;
import db.datastore.TableHeader;
import db.operators.NaryNode;
import db.query.TablePair;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Logical operator for join handling : keeps track of left and right tuple sources and optional join condition.
 */
public class LogicalJoinOperator implements LogicalOperator, NaryNode<LogicalOperator> {
    private final List<LogicalOperator> children;
    private UnionFind unionFind;
    private List<Pair<TablePair, Expression>> unusedExpressions;
    private TableHeader outputSchema;

    /**
     * @param children          the child nodes to be joined
     * @param unionFind         the union find with constraints
     * @param unusedExpressions join expressions which dont otherwise fit into the union find.
     */
    public LogicalJoinOperator(List<LogicalOperator> children, UnionFind unionFind, List<Pair<TablePair, Expression>> unusedExpressions) {
        this.children = children;
        this.unionFind = unionFind;
        this.unusedExpressions = unusedExpressions;

        if (this.children.size() == 0) {
            this.outputSchema = new TableHeader();
        } else {
            List<TableHeader> headers = children.stream().map(LogicalOperator::getHeader).collect(Collectors.toList());
            this.outputSchema = computeHeader(headers);
        }
    }

    /**
     * Compute the result header for this join.
     *
     * @param left  the left node.
     * @param right the right node.
     * @return The resultant header.
     */
    public static TableHeader computeHeader(TableHeader left, TableHeader right) {
        int tableWidth = left.size() + right.size();

        List<String> tableIdentifiers = new ArrayList<>(tableWidth);
        List<String> columnNames = new ArrayList<>(tableWidth);

        columnNames.addAll(left.columnNames);
        columnNames.addAll(right.columnNames);

        tableIdentifiers.addAll(left.tableIdentifiers);
        tableIdentifiers.addAll(right.tableIdentifiers);

        return new TableHeader(tableIdentifiers, columnNames);
    }

    /**
     * Compute the result header for a N-way join.
     *
     * @param sourceHeaders List of ordered headers from source operators
     * @return The resulting left-to-right join header.
     */
    public static TableHeader computeHeader(List<TableHeader> sourceHeaders) {
        List<String> tableIdentifiers = sourceHeaders.stream()
                .map(h -> h.tableIdentifiers)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        List<String> columnNames = sourceHeaders.stream()
                .map(h -> h.columnNames)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return new TableHeader(tableIdentifiers, columnNames);
    }

    /**
     * @return the union find with all of the expressions
     */
    public UnionFind getUnionFind() {
        return unionFind;
    }

    /**
     * @return the list of unused expressions
     */
    public List<Pair<TablePair, Expression>> getUnusedExpressions() {
        return new ArrayList<>(unusedExpressions);
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return outputSchema;
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
    public List<LogicalOperator> getChildren() {
        return this.children;
    }
}
