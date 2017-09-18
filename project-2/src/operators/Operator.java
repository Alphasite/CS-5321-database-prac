package operators;

import datastore.Tuple;

import java.util.Optional;

public interface Operator {
    Optional<Tuple> getNextTuple();
    boolean reset();
}
