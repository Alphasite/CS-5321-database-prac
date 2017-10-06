package db.operators.logical;

import net.sf.jsqlparser.expression.Expression;

public class LogicalJoinOperator implements LogicalOperator {
    private final LogicalOperator left;
    private final LogicalOperator right;
    private final Expression joinCondition;

    public LogicalJoinOperator(LogicalOperator left, LogicalOperator right, Expression joinCondition) {
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
    }
}
