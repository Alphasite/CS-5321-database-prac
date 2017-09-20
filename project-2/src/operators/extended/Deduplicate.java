package operators.extended;

import datastore.Tuple;
import operators.AbstractOperator;

import java.util.Optional;

public class Deduplicate extends AbstractOperator {
    @Override
    public Optional<Tuple> getNextTuple() {
        return Optional.empty();
    }

    @Override
    public boolean reset() {
        return false; //TODO
    }
}
