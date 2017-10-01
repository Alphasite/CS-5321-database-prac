package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import net.sf.jsqlparser.expression.Expression;
import operators.Operator;
import query.ExpressionEvaluator;

/**
 * @inheritDoc
 */
public class SelectionOperator implements Operator {
    private Operator source;
    private ExpressionEvaluator evaluator;

    /**
     * @param source The source operator which is to be filtered.
     * @param expression The expression which is evaluated for every tuple to check whether
     *                   or not the tuple should returned from this function.
     */
    public SelectionOperator(Operator source, Expression expression) {
        this.source = source;
        this.evaluator = new ExpressionEvaluator(expression, getHeader());
    }

    /** The operator produces only tuples which match the provided expression.
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        Tuple next;

        while ((next = this.source.getNextTuple()) != null) {
            if (evaluator.matches(next))
                return next;
        }
        return null;

    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        return this.source.reset();
    }

    public Expression getPredicate() {
        return evaluator.getExpression();
    }
}
