package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import net.sf.jsqlparser.expression.Expression;
import operators.Operator;
import query.ExpressionEvaluator;

public class Selection implements Operator {
    private Operator source;
    private ExpressionEvaluator evaluator;

    public Selection(Operator source, Expression expression) {
        this.source = source;
        this.evaluator = new ExpressionEvaluator(expression, getHeader());
    }

    @Override
    public Tuple getNextTuple() {
        Tuple next;

        while ((next = this.source.getNextTuple()) != null) {
            if (evaluator.matches(next))
                return next;
        }
        return null;

    }

    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    @Override
    public boolean reset() {
        return this.source.reset();
    }

}
