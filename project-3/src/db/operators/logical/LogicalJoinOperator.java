package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.BinaryNode;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical operator for join handling : keeps track of left and right tuple sources and optional join condition.
 */
public class LogicalJoinOperator implements LogicalOperator, BinaryNode<LogicalOperator> {
    private final LogicalOperator left;
    private final LogicalOperator right;
    private final Expression joinCondition;
    private final TableHeader outputSchema;

    public LogicalJoinOperator(LogicalOperator left, LogicalOperator right, Expression joinCondition) {
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
        this.outputSchema = computeHeader(left.getHeader(), right.getHeader());
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
     * Return an optional expression which describes whether or not a tuple should be joned
     *
     * @return The nullable expression.
     */
    public Expression getJoinCondition() {
        return joinCondition;
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
    public LogicalOperator getLeft() {
        return left;
    }

    /**
     * @inheritDoc
     */
    @Override
    public LogicalOperator getRight() {
        return right;
    }
}
