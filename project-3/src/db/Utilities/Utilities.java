package db.Utilities;

import db.datastore.tuple.Tuple;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A collection of unrelated utility methods.
 */
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
     * Compare two lists by comparing fields in the specified order
     * @param keysA list of indices used to compare list A
     * @param keysB list of indices used to compare list B
     * @return Standard comparator contract : 0 if A == B, -1 if A < B, 1 if A > B
     */
    public static int compareLists(List<Integer> A, List<Integer> B, List<Integer> keysA, List<Integer> keysB) {
        if (A == B)
            return 0;

        assert keysA.size() == keysB.size();

        Iterator<Integer> iterA = keysA.iterator();
        Iterator<Integer> iterB = keysB.iterator();

        while (iterA.hasNext()) {
            int a = A.get( iterA.next() );
            int b = B.get( iterB.next() );

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
        int currentMin = -1;

        // First pass : check if all values are null
        for (int i = 0; i < tuples.size(); i++) {
            if (tuples.get(i) != null) {
                currentMin = i;
                break;
            }
        }

        if (currentMin == -1) {
            throw new RuntimeException("All tuples are null");
        }

        for (int i = currentMin + 1; i < tuples.size(); i++) {
            Tuple t = tuples.get(i);
            if (t != null && comparator.compare(t, tuples.get(currentMin)) < 0) {
                currentMin = i;
            }
        }

        return currentMin;
    }

    /**
     * There the files in the specified directory.
     *
     * @param directory the directory to empty.
     */
    public static void cleanDirectory(Path directory) {
        try {
            Files.walk(directory).filter(Files::isRegularFile).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
