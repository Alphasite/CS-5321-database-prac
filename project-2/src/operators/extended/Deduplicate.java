package operators.extended;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.io.PrintStream;

public class Deduplicate implements Operator {
    private Operator source;

    public Deduplicate(Operator source) {
        this.source = source;
    }

    @Override
    public Tuple getNextTuple() {
        //TODO:
        return null;
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
