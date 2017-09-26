package operators.extended;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.io.PrintStream;
import java.util.Optional;

public class Sort implements Operator {
    private Operator source;

    public Sort(Operator source) {
        this.source = source;
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        return Optional.empty();
    }

    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    @Override
    public boolean reset() {
        return this.source.reset();
    }

    @Override
    public void dump(PrintStream stream) {

    }
}
