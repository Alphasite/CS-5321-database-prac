package operators.bag;

import datastore.Tuple;
import operators.AbstractOperator;
import operators.Operator;

import java.util.Optional;

public class Selection extends AbstractOperator {
    Operator source;

    public Selection(Operator source) {
        this.source = source;
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
    public boolean reset() {
        return this.source.reset();
    }
}
