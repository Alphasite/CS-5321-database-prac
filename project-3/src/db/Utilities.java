package db;

import db.datastore.tuple.Tuple;
import net.sf.jsqlparser.schema.Table;

import java.util.Comparator;
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

    /**
     * Compare two lists by comparing fields in the specified order
     * @param keys list of indices used to compare both lists
     * @return Standard comparator contract : 0 if A == B, -1 if A < B, 1 if A > B
     */
    public static int compareLists(List<Integer> A, List<Integer> B, List<Integer> keys) {
        if (A == B)
            return 0;

        for (int i : keys) {
            int a = A.get(i);
            int b = B.get(i);

            int result = Integer.compare(a, b);

            if (result != 0) {
                return result;
            }
        }

        return 0;
    }

    /**
     * Get the index of the tuple which has the lowest value according to the provided comparator
     * @return index of lowest order tuple in list
     */
    public static int getFirstTuple(List<Tuple> tuples, Comparator<Tuple> comparator) {
        int currentMin = 0;

        for (int i = 1; i < tuples.size(); i++) {
            if (comparator.compare(tuples.get(i), tuples.get(currentMin)) > 0) {
                currentMin = i;
            }
        }

        return currentMin;
    }
}
