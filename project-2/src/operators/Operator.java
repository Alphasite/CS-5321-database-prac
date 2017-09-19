package operators;

import datastore.TableHeader;
import datastore.Tuple;

import java.util.Optional;

public interface Operator {
    Optional<Tuple> getNextTuple();
    TableHeader getHeader();
    boolean reset();
}
