package operators.extended;

import datastore.Tuple;
import operators.Operator;

import java.util.Optional;

public class Sort implements Operator {
    @Override
    public Optional<Tuple> getNextTuple() {
        return Optional.empty();
    }

    @Override
    public boolean reset() {
        return false; // TODO
    }
}
