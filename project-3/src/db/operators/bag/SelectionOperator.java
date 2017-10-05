package db.operators.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.Operator;
import db.query.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

/**
 * This operator filters its input according to a predicate specified as an {@link Expression}
 *
 * @inheritDoc
 */
public class SelectionOperator implements Operator {
    private final Operator source;

    private ExpressionEvaluator evaluator;

    /**
     * @param source     The source operator which is to be filtered.
     * @param expression The expression which is evaluated for every tuple to check whether
     *                   or not the tuple should returned from this function.
     */
    public SelectionOperator(Operator source, Expression expression) {
        this.source = source;
        this.evaluator = new ExpressionEvaluator(expression, getHeader());
    }

    /**
     * The operator produces only tuples which match the provided expression.
     *
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
