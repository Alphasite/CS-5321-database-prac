package db.operators.physical.bag;

import db.TestUtils;
import db.Utilities.Utilities;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.physical.Operator;
import db.operators.physical.extended.ExternalSortOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortMergeJoinTest {
    private List<Tuple> tuplesLeft;
    private TableHeader headerLeft;
    private DummyOperator opLeft;

    private List<Tuple> tuplesRight;
    private TableHeader headerRight;
    private DummyOperator opRight;

    @Before
    public void setUp() {
        tuplesLeft = new ArrayList<>();
        tuplesLeft.add(new Tuple(Arrays.asList(10, 5, 3)));
        tuplesLeft.add(new Tuple(Arrays.asList(5, 10, 10)));
        tuplesLeft.add(new Tuple(Arrays.asList(8, 5, 0)));
        tuplesLeft.add(new Tuple(Arrays.asList(10, 10, 10)));
        tuplesLeft.add(new Tuple(Arrays.asList(5, 3, 2)));
        tuplesLeft.add(new Tuple(Arrays.asList(5, 5, 1)));
        headerLeft = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        opLeft = new DummyOperator(tuplesLeft, headerLeft);

        tuplesRight = new ArrayList<>();
        tuplesRight.add(new Tuple(Arrays.asList(3, 1, 3)));
        tuplesRight.add(new Tuple(Arrays.asList(5, 3, 10)));
        tuplesRight.add(new Tuple(Arrays.asList(3, 5, 0)));
        tuplesRight.add(new Tuple(Arrays.asList(10, 5, 3)));
        tuplesRight.add(new Tuple(Arrays.asList(5, 2, 0)));
        tuplesRight.add(new Tuple(Arrays.asList(5, 5, 1)));
        headerRight = new TableHeader(Arrays.asList("Boats", "Boats", "Boats"), Arrays.asList("D", "E", "F"));
        opRight = new DummyOperator(tuplesRight, headerRight);
    }

    @Test
    public void testSingleColumn() {
        TableHeader sortHeaderLeft = new TableHeader(
                Arrays.asList("Sailors"),
                Arrays.asList("B")
        );
        TableHeader sortHeaderRight = new TableHeader(
                Arrays.asList("Boats"),
                Arrays.asList("D")
        );

        ExternalSortOperator sortedLeft = new ExternalSortOperator(opLeft, sortHeaderLeft, 3, TestUtils.TEMP_PATH);
        ExternalSortOperator sortedRight = new ExternalSortOperator(opRight, sortHeaderRight, 3, TestUtils.TEMP_PATH);
        SortMergeJoinOperator smj = new SortMergeJoinOperator(sortedLeft, sortedRight, null);

        HashSet<Tuple> tuplesRef = new HashSet<>();
        tuplesRef.add(new Tuple(Arrays.asList(10, 5, 3, 5, 3, 10)));
        tuplesRef.add(new Tuple(Arrays.asList(10, 5, 3, 5, 2, 0)));
        tuplesRef.add(new Tuple(Arrays.asList(10, 5, 3, 5, 5, 1)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 10, 10, 10, 5, 3)));
        tuplesRef.add(new Tuple(Arrays.asList(8, 5, 0, 5, 3, 10)));
        tuplesRef.add(new Tuple(Arrays.asList(8, 5, 0, 5, 2, 0)));
        tuplesRef.add(new Tuple(Arrays.asList(8, 5, 0, 5, 5, 1)));
        tuplesRef.add(new Tuple(Arrays.asList(10, 10, 10, 10, 5, 3)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 3, 2, 3, 1, 3)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 3, 2, 3, 5, 0)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 5, 1, 5, 3, 10)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 5, 1, 5, 2, 0)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 5, 1, 5, 5, 1)));

        int i = 0;
        while (smj.hasNextTuple()) {
            assertTrue(tuplesRef.contains(smj.getNextTuple()));
            i++;
        }

        assertEquals(tuplesRef.size(), i);

        smj.close();
    }

    @Test
    public void testTwoColumns() {
        TableHeader sortHeaderLeft = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("A", "B")
        );
        TableHeader sortHeaderRight = new TableHeader(
                Arrays.asList("Boats", "Boats"),
                Arrays.asList("D", "E")
        );

        ExternalSortOperator sortedLeft = new ExternalSortOperator(opLeft, sortHeaderLeft, 3, TestUtils.TEMP_PATH);
        ExternalSortOperator sortedRight = new ExternalSortOperator(opRight, sortHeaderRight, 3, TestUtils.TEMP_PATH);
        SortMergeJoinOperator smj = new SortMergeJoinOperator(sortedLeft, sortedRight, null);

        HashSet<Tuple> tuplesRef = new HashSet<>();
        tuplesRef.add(new Tuple(Arrays.asList(10, 5, 3, 10, 5, 3)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 3, 2, 5, 3, 10)));
        tuplesRef.add(new Tuple(Arrays.asList(5, 5, 1, 5, 5, 1)));

        int i = 0;
        while (smj.hasNextTuple()) {
            assertTrue(tuplesRef.contains(smj.getNextTuple()));
            i++;
        }

        assertEquals(tuplesRef.size(), i);

        smj.close();
    }

    @Test
    public void testWorstCaseScenario() {
        ArrayList<Tuple> tuplesWorstCase = new ArrayList<>();
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 1)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 2)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 3)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 4)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 5)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 6)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 7)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 8)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 9)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 10)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 11)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 12)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 13)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 14)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 15)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 16)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 17)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 18)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 19)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 20)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 21)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 22)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 23)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 24)));
        tuplesWorstCase.add(new Tuple(Arrays.asList(10, 25)));

        TableHeader headerWorstLeft = new TableHeader(Arrays.asList("R1", "R1"), Arrays.asList("G", "H"));
        TableHeader headerWorstRight = new TableHeader(Arrays.asList("R2", "R2"), Arrays.asList("G", "H"));
        Operator opLeft = new DummyOperator(tuplesWorstCase, headerWorstLeft);
        Operator opRight = new DummyOperator(tuplesWorstCase, headerWorstRight);

        TableHeader sortHeaderLeft = new TableHeader(
                Arrays.asList("R1"),
                Arrays.asList("G")
        );
        TableHeader sortHeaderRight = new TableHeader(
                Arrays.asList("R2"),
                Arrays.asList("G")
        );

        ExternalSortOperator sortedLeft = new ExternalSortOperator(opLeft, sortHeaderLeft, 3, TestUtils.TEMP_PATH);
        ExternalSortOperator sortedRight = new ExternalSortOperator(opRight, sortHeaderRight, 3, TestUtils.TEMP_PATH);
        SortMergeJoinOperator smj = new SortMergeJoinOperator(sortedLeft, sortedRight, null);

        HashSet<Tuple> tuplesRef = new HashSet<>();
        for (int i = 1; i <= 25; i++) {
            for (int j = 1; j <= 25; j++) {
                tuplesRef.add(new Tuple(Arrays.asList(10, i, 10, j)));
            }
        }

        int i = 0;
        while (smj.hasNextTuple()) {
            assertTrue(tuplesRef.contains(smj.getNextTuple()));
            i++;
        }

        assertEquals(tuplesRef.size(), i);

        smj.close();
    }
    @After
    public void cleanup() {
        Utilities.cleanDirectory(TestUtils.TEMP_PATH);
    }
}
