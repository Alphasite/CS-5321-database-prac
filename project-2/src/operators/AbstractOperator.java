package operators;

import datastore.Tuple;

import java.io.PrintStream;

public abstract class AbstractOperator implements Operator {

    @Override
    public void dump(PrintStream stream) {
        stream.println(getHeader());

        Tuple record = getNextTuple();
        int i = 0;

        while (record!=null) {
            stream.println(i++ + ": " + record);
            record = getNextTuple();
        }
    }
}
