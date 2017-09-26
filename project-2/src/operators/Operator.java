package operators;

import datastore.TableHeader;
import datastore.Tuple;

import java.io.PrintStream;
import java.util.Optional;

public interface Operator {
    Optional<Tuple> getNextTuple();
    TableHeader getHeader();
    boolean reset();

    void dump(PrintStream stream);
}
