package operators;

import datastore.TableHeader;
import datastore.Tuple;

import java.io.PrintStream;

public interface Operator {
    Tuple getNextTuple();
    TableHeader getHeader();
    boolean reset();

    default int dump(PrintStream stream) {
        stream.println(this.getHeader());

        int i = 0;
        Tuple record;
        while ((record = this.getNextTuple()) != null) {
            stream.println(++i + ": " + record);
        }

        return i;
    }
}
