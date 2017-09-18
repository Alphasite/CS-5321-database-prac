package operators.bag;

import datastore.Tuple;
import operators.Operator;

import java.util.Optional;

public class Projection implements Operator {
    @Override
    public Optional<Tuple> getNextTuple() {
        return Optional.empty();
    }

    @Override
    public boolean reset() {
        return false; // TODO
    }
}
