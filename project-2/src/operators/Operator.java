package operators;

import datastore.TableHeader;
import datastore.Tuple;

import java.io.PrintStream;
import java.util.Optional;

public interface Operator {
    Optional<Tuple> getNextTuple();
    TableHeader getHeader();
    boolean reset();

    default int dump(PrintStream stream) {
        stream.println(this.getHeader());

        int i = 0;
        Optional<Tuple> record;
        while ((record = this.getNextTuple()).isPresent()) {
            stream.println(++i + ": " + record.get());
        }

        return i;
    }
}
