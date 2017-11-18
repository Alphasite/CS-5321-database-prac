package db.operators.physical.extended;

import db.Utilities.Utilities;
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

    private List<Integer> comparisonKeys1;
    private List<Integer> comparisonKeys2;

    /**
     * Build a comparator that compares fields in the order specified by the sortHeader parameter.
     * If two tuples match on the sorting fields they are considered equal (ie. no tie resolution on the other fields)
     * @param sortHeader1 The header which defines the sort order of the left tuple
     * @param tupleHeader1 Header structure of the right tuples to be compared
     * @param sortHeader2 The header which defines the sort order of the right tuple
     * @param tupleHeader2 Header structure of the right tuples to be compared
     */
    public TupleComparator(TableHeader sortHeader1, TableHeader tupleHeader1, TableHeader sortHeader2, TableHeader tupleHeader2) {
        comparisonKeys1 = new ArrayList<>();
        comparisonKeys2 = new ArrayList<>();

        // Build the internal comparison key list
        for (int i = 0; i < sortHeader1.size(); i++) {
            String alias1 = sortHeader1.tableIdentifiers.get(i);
            String header1 = sortHeader1.columnNames.get(i);

            String alias2 = sortHeader2.tableIdentifiers.get(i);
            String header2 = sortHeader2.columnNames.get(i);

            Optional<Integer> index1 = tupleHeader1.resolve(alias1, header1);
            Optional<Integer> index2 = tupleHeader2.resolve(alias2, header2);

            if (index1.isPresent() && index2.isPresent()) {
                comparisonKeys1.add(index1.get());
                comparisonKeys2.add(index2.get());
            } else {
                throw new RuntimeException("Sort mappings are incorrect. " + alias1 + "." + header1 + " or " + alias2 + "." + header2 + " has no match.");
            }
        }
    }

    /**
     * Build a comparator that compares fields in the order specified by the sortHeader parameter.
     * If two tuples match on the sorting fields they are considered equal (ie. no tie resolution on the other fields)
     * @param sortHeader the header used to sort relation
     * @param tupleHeader Header structure of the tuples to be compared
     */
    public TupleComparator(TableHeader sortHeader, TableHeader tupleHeader) {
        this(sortHeader, tupleHeader, sortHeader, tupleHeader);
    }

    /**
     * @inheritDoc
     */
    @Override
    public int compare(Tuple o1, Tuple o2) {
        return Utilities.compareLists(o1.fields, o2.fields, comparisonKeys1, comparisonKeys2);
    }
}
