package operators;

import datastore.Tuple;

import java.io.PrintStream;
import java.util.Optional;

public interface Operator {
    Optional<Tuple> getNextTuple();
    boolean reset();

    void dump(PrintStream stream);
}
