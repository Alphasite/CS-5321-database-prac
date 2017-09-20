package operators.bag;

import datastore.Tuple;
import operators.AbstractOperator;

import java.util.Optional;

public class Projection extends AbstractOperator {
    @Override
    public Optional<Tuple> getNextTuple() {
        return Optional.empty();
    }

    @Override
    public boolean reset() {
        return false; // TODO
    }
}
