package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import net.sf.jsqlparser.expression.Expression;
import operators.Operator;

import java.io.PrintStream;
import java.util.Optional;

public class Selection implements Operator {
    Operator source;
    Expression expression;

    public Selection(Operator source, Expression expression) {
        this.source = source;
        this.expression = expression;
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        Optional<Tuple> next;

        while ((next = this.source.getNextTuple()).isPresent()) {
            // TODO
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

    @Override
    public void dump(PrintStream stream) {

    }
}
