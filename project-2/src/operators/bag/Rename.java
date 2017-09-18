package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;
import java.util.Optional;

public class Rename implements Operator {
    private Operator child;
    private String newTableName;
    private TableHeader renamedTableHeaderCache;

    public Rename(Operator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;
        this.renamedTableHeaderCache = null;
    }

    @Override
    public Optional<Tuple> getNextTuple() {
        Optional<Tuple> nextTuple = this.child.getNextTuple();

        if (nextTuple.isPresent()) {
            if (renamedTableHeaderCache == null) {
                ArrayList<String> newAliases = new ArrayList<>();
                for (int i = 0; i < nextTuple.get().header.aliasMap.size(); i++) {
                    newAliases.add(this.newTableName);
                }

                this.renamedTableHeaderCache = new TableHeader(newAliases, nextTuple.get().header.columnHeaders);
            }

            return Optional.of(new Tuple(this.renamedTableHeaderCache, nextTuple.get().fields));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean reset() {
        this.renamedTableHeaderCache = null;
        return this.child.reset();
    }
}
