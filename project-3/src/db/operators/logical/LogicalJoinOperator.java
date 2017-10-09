package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.BinaryNode;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;

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

    public static TableHeader computeHeader(TableHeader left, TableHeader right) {
        int tableWidth = left.size() + right.size();

        List<String> headings = new ArrayList<>(tableWidth);
        List<String> aliases = new ArrayList<>(tableWidth);

        headings.addAll(left.columnHeaders);
        headings.addAll(right.columnHeaders);

        aliases.addAll(left.columnAliases);
        aliases.addAll(right.columnAliases);

        return new TableHeader(aliases, headings);
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    @Override
    public TableHeader getHeader() {
        return outputSchema;
    }

    @Override
    public void accept(LogicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public LogicalOperator getLeft() {
        return left;
    }

    @Override
    public LogicalOperator getRight() {
        return right;
    }
}
