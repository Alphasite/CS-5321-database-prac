package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Join implements Operator {
    Operator left;
    Operator right;

    private Tuple leftTupleCache;
    private TableHeader tableHeader;

    public Join(Operator left, Operator right) {
        this.left = left;
        this.right = right;
        this.reset();

        int tableWidth = left.getHeader().size() + right.getHeader().size();

        List<String> headings = new ArrayList<>(tableWidth);
        List<String> aliases = new ArrayList<>(tableWidth);

        headings.addAll(left.getHeader().columnHeaders);
        headings.addAll(right.getHeader().columnHeaders);

        aliases.addAll(left.getHeader().columnAliases);
        aliases.addAll(right.getHeader().columnAliases);

        this.tableHeader = new TableHeader(aliases, headings);
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        // TODO make this more efficient.
        // we probably can do selections inline (as an optimisation)?
        // Also i wish we had value types, this would be much faster.

        // Get the rhs tuple and if necessary wrap around the lhs
        Optional<Tuple> rightFromChild;

        while (!(rightFromChild = this.right.getNextTuple()).isPresent()) {
            if (!loadNextLeftTuple()) {
                return Optional.empty();
            }
        }

        Tuple left = leftTupleCache;
        Tuple right = rightFromChild.get();

        return Optional.of(left.join(right));
    }

    @Override
    public TableHeader getHeader() {
        return this.tableHeader;
    }

    @Override
    public boolean reset() {
        if (this.left.reset() && this.right.reset()) {
            this.loadNextLeftTuple();
            return true;
        } else {
            return false;
        }
    }

    private boolean loadNextLeftTuple() {
        Optional<Tuple> leftFromChild = this.left.getNextTuple();
        if (leftFromChild.isPresent()) {
            leftTupleCache = leftFromChild.get();
            right.reset();
            return true;
        } else {
            return false;
        }
    }

}
