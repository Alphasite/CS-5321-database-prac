package operators.bag;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;

/** Rename the table of the child tuples' schema.
 *
 * This operator changes the alias field of the header.
 *
 * @inheritDoc
 */
public class RenameOperator implements Operator {
    private Operator child;
    private String newTableName;
    private TableHeader header;


    /**
     * @param child The child operator.
     * @param newTableName The new name of the table.
     */
    public RenameOperator(Operator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;

        ArrayList<String> newAliases = new ArrayList<>();
        for (int i = 0; i < this.child.getHeader().size(); i++) {
            newAliases.add(this.newTableName);
        }

        this.header = new TableHeader(newAliases, child.getHeader().columnHeaders);
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        return this.child.getNextTuple();
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return header;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        return this.child.reset();
    }
}