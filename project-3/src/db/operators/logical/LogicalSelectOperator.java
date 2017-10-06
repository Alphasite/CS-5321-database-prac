package db.operators.logical;

import net.sf.jsqlparser.expression.Expression;

public class LogicalSelectOperator implements LogicalOperator {
    private final LogicalOperator source;
    private final Expression selectCondition;

    public LogicalSelectOperator(LogicalOperator source, Expression selectCondition) {
        this.source = source;
        this.selectCondition = selectCondition;
    }
}
