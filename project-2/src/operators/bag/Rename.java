package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;

public class Rename implements Operator {
    private Operator child;
    private String newTableName;
    private TableHeader header;

    public Rename(Operator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;

        ArrayList<String> newAliases = new ArrayList<>();
        for (int i = 0; i < this.child.getHeader().size(); i++) {
            newAliases.add(this.newTableName);
        }

        this.header = new TableHeader(newAliases, child.getHeader().columnHeaders);
    }

    @Override
    public Tuple getNextTuple() {
        return this.child.getNextTuple();
    }

    @Override
    public TableHeader getHeader() {
        return header;
    }

    @Override
    public boolean reset() {
        return this.child.reset();
    }
}
