package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import net.sf.jsqlparser.expression.Expression;
import operators.Operator;
import query.ExpressionEvaluator;

import java.util.Optional;

public class Selection implements Operator {
    private Operator source;
    private ExpressionEvaluator evaluator;

    public Selection(Operator source, Expression expression) {
        this.source = source;
        this.evaluator = new ExpressionEvaluator(expression);
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        Optional<Tuple> next;

        while ((next = this.source.getNextTuple()).isPresent()) {
            if (evaluator.matches(next.get()))
                return next;
        }

        return Optional.empty();
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
