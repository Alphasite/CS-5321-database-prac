package operators;

import datastore.Tuple;

import java.io.PrintStream;
import java.util.Optional;

public abstract class AbstractOperator implements Operator {

    @Override
    public void dump(PrintStream stream) {
        Optional<Tuple> record = getNextTuple();
        int i = 0;

        while (record.isPresent()) {
            stream.println(i++ + ": " + record.get());
            record = getNextTuple();
        }
    }
}
