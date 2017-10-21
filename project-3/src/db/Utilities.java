package db;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import net.sf.jsqlparser.schema.Table;

import java.util.List;

public class Utilities {
    /**
     * Get the real identifier of the table, so resolve aliases if they exist, etc.
     *
     * @param table The table to have its name retrieved.
     * @return The tables 'real' name.
     */
    public static String getIdentifier(Table table) {
        if (table.getAlias() != null) {
            return table.getAlias();
        } else {
            return table.getName();
        }
    }

    public static int compareTuples(Tuple a, Tuple b, TableHeader sortHeader) {
        if (a == b)
            return 0;

        // TODO: finish this
        return 0;
    }

    public static int getFirstTuple(List<Tuple> tuples, TableHeader sortHeader) {
        int currentMin = 0;

        for (int i = 1; i < tuples.size(); i++) {
            if (compareTuples(tuples.get(i), tuples.get(currentMin), sortHeader) > 0) {
                currentMin = i;
            }
        }

        return currentMin;
    }
}
