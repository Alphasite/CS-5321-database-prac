package operators;

import datastore.TableHeader;
import datastore.Tuple;

import java.io.PrintStream;

public interface Operator {
    Tuple getNextTuple();
    TableHeader getHeader();
    boolean reset();

    void dump(PrintStream stream);
}
