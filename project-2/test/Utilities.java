import datastore.Tuple;
import operators.Operator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * A collection of utility methods for the testing suite.
 */
public class Utilities {

    /**
     * A method to compare the output of two operators, e.g. a reference and processed.
     * @param reference The expected output operator
     * @param tested The generated operator
     */
    public static void compareOperators(Operator reference, Operator tested) {
        System.out.println(tested.getHeader());


        int i = 0;
        while (true) {
            Tuple ref = reference.getNextTuple();
            Tuple test = tested.getNextTuple();

            assertThat(ref, equalTo(test));

            if (ref == null || test == null) {
                break;
            }

            System.out.println("[OK] " + ++i + " " + ref.toString());
        }

        System.out.println("ALL OKAY: checked " + i + " rows.");
    }
}
