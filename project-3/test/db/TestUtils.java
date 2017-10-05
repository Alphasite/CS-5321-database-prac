package db;

import db.datastore.tuple.Tuple;
import db.operators.Operator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

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

            assertThat(test, equalTo(ref));

            if (ref == null || test == null) {
                break;
            }

            System.out.println("[OK] " + ++i + " " + ref.toString());
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
