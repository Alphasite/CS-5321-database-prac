package db;

import db.datastore.tuple.Tuple;
import db.operators.physical.Operator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.fail;

/**
 * A collection of utility methods for the testing suite.
 */
public class TestUtils {

    /**
     * A method to compare the tuple output of two db.operators, e.g. a reference and processed.
     *
     * @param reference The expected output operator
     * @param tested    The generated operator
     */
    public static void compareTuples(Operator reference, Operator tested) {
        System.out.println(tested.getHeader());


        int i = 0;
        while (true) {
            Tuple ref = reference.getNextTuple();
            Tuple test = tested.getNextTuple();

            if (ref == null && test == null) {
                break;
            } else if (test == null) {
                fail("output has fewer tuples than expected");
            } else if (ref == null) {
                fail("output has more tuples than expected");
            } else {
                assertThat(test, equalTo(ref));
            }

            i++;
//            System.out.println("[OK] " + ++i + " " + ref.toString());
        }

        System.out.println("ALL OKAY: checked " + i + " rows.");
    }

    /**
     * A method to compare the tuple output of two db.operators, e.g. a reference and processed.
     * <p>
     * This maintains an internal buffer to enable unordered comparisons.
     *
     * @param reference The expected output operator
     * @param tested    The generated operator
     */
    public static void unorderedCompareTuples(Operator reference, Operator tested) {
        System.out.println(tested.getHeader());

        int referenceTupleCount = 0;
        Set<String> referenceTuples = new HashSet<>();
        List<String> testedTuples = new ArrayList<>();

        Tuple tuple;

        while ((tuple = reference.getNextTuple()) != null) {
            referenceTupleCount += 1;
            referenceTuples.add(tuple.toString());
        }

        while ((tuple = tested.getNextTuple()) != null) {
            testedTuples.add(tuple.toString());
        }

        if (referenceTupleCount > testedTuples.size()) {
            assertThat("output has fewer tuples than expected", referenceTuples.size(), equalTo(testedTuples.size()));
        }

        if (referenceTupleCount < testedTuples.size()) {
            assertThat("output has more tuples than expected", referenceTuples.size(), equalTo(testedTuples.size()));
        }

        int i = 0;
        for (String testedTuple : testedTuples) {
            assertThat(referenceTuples.contains(testedTuple), equalTo(true));
            i++;
        }

        System.out.println("ALL OKAY: checked " + i + " rows.");
    }

    public static PlainSelect parseQuery(String query) {
        CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
        PlainSelect select;

        try {
            select = (PlainSelect) parser.Select().getSelectBody();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }

        return select;
    }

    public static int countNotNullTuples(Operator op) {
        int i = 0;
        Tuple record;
        while ((record = op.getNextTuple()) != null) {
            i += 1;

            assertThat(record, notNullValue());
        }

        return i;
    }
}
