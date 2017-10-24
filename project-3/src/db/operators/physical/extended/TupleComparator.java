package db.operators.physical.extended;

import db.Utilities;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * A tuple comparator used by the sorting operators
 */
public class TupleComparator implements Comparator<Tuple> {

    private List<Integer> comparisonKeys;

    /**
     * Build a comparator that compares fields in the order specified by the sortHeader parameter.
     * If two tuples match on the sorting fields they are considered equal (ie. no tie resolution on the other fields)
     * @param sortHeader
     * @param tupleHeader Header structure of the tuples to be compared
     */
    public TupleComparator(TableHeader sortHeader, TableHeader tupleHeader) {
        comparisonKeys = new ArrayList<>();

        // Build the internal comparison key list
        for (int i = 0; i < sortHeader.size(); i++) {
            String alias = sortHeader.columnAliases.get(i);
            String header = sortHeader.columnHeaders.get(i);

            Optional<Integer> index = tupleHeader.resolve(alias, header);

            if (index.isPresent()) {
                comparisonKeys.add(index.get());
            } else {
                throw new RuntimeException("Sort mappings are incorrect. " + alias + "." + header + " has no match.");
            }
        }

        // TODO: add remaining fields ?
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
        return Utilities.compareLists(o1.fields, o2.fields, comparisonKeys);
    }
}
