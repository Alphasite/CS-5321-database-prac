package db.operators.logical;

import db.datastore.TableHeader;
import db.operators.physical.Operator;

import java.util.ArrayList;

public class LogicalRenameOperator {
    private final LogicalOperator child;
    private String newTableName;

    public LogicalRenameOperator(LogicalOperator child, String newTableName) {
        this.child = child;
        this.newTableName = newTableName;
    }

    public static TableHeader computeHeader(Operator child, String newTableName) {
        ArrayList<String> newAliases = new ArrayList<>();
        for (int i = 0; i < child.getHeader().size(); i++) {
            newAliases.add(newTableName);
        }

        return new TableHeader(newAliases, child.getHeader().columnHeaders);
    }
}
