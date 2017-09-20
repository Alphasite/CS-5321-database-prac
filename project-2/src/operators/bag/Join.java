package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.AbstractOperator;
import operators.Operator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Optional;

public class Join extends AbstractOperator {
    Operator left;
    Operator right;

    private Tuple leftTupleCache;
    private TableHeader tableHeaderCache;

    public Join(Operator left, Operator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        // TODO make this more efficient.
        // we probably can do selections inline (as an optimisation)?
        // Also i wish we had value types, this would be much faster.

        // Populate the initial LHS cache
        if (leftTupleCache == null) {
            Optional<Tuple> leftFromChild = this.left.getNextTuple();
            if (leftFromChild.isPresent()) {
                leftTupleCache = leftFromChild.get();
            } else {
                return Optional.empty();
            }
        }

        // Get the rhs tuple and if necessary wrap around the lhs
        Optional<Tuple> rightFromChild;

        while (!(rightFromChild = this.right.getNextTuple()).isPresent()) {
            Optional<Tuple> leftFromChild = this.left.getNextTuple();
            if (leftFromChild.isPresent()) {
                leftTupleCache = leftFromChild.get();
                right.reset();
            } else {
                return Optional.empty();
            }
        }

        Tuple left = leftTupleCache;
        Tuple right = rightFromChild.get();

        if (this.tableHeaderCache == null) {
            int tableWidth = left.header.columnHeaders.size() + right.header.columnHeaders.size();

            ArrayList<String> headings = new ArrayList<>(tableWidth);
            ArrayList<String> aliases = new ArrayList<>(tableWidth);

            headings.addAll(left.header.columnHeaders);
            headings.addAll(right.header.columnHeaders);

            aliases.addAll(left.header.columnAliases);
            aliases.addAll(right.header.columnAliases);

            this.tableHeaderCache = new TableHeader(aliases, headings);
        }

        return Optional.of(left.join(this.tableHeaderCache, right));
    }

    @Override
    public boolean reset() {
        if (this.left.reset() && this.right.reset()) {
            this.tableHeaderCache = null;
            this.leftTupleCache = null;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void dump(PrintStream stream) {
//        stream.println(getNextTuple().get().header);
        super.dump(stream);
    }
}
