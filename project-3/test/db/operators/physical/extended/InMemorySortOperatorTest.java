package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.logical.LogicalSortOperator;
import db.operators.physical.Operator;
import db.operators.physical.SeekableOperator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;

/**
 * Test reordering of tuples by the sort operator
 * Pay particular attention to conflict resolution as explained in section 2.1
 */
public class InMemorySortOperatorTest {
    private List<Tuple> tuplesA;
    private TableHeader headerA;
    private DummyOperator opA;

    @Before
    public void setUp() {
        tuplesA = new ArrayList<>();
        tuplesA.add(new Tuple(Arrays.asList(1, 2, 3)));
        tuplesA.add(new Tuple(Arrays.asList(5, 1, 1)));
        tuplesA.add(new Tuple(Arrays.asList(2, 1, 3)));
        tuplesA.add(new Tuple(Arrays.asList(4, 3, 2)));
        tuplesA.add(new Tuple(Arrays.asList(3, 3, 2)));
        tuplesA.add(new Tuple(Arrays.asList(1, 2, 1)));
        headerA = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        opA = new DummyOperator(tuplesA, headerA);
    }

    @Test
    public void testSingleColumn() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors"),
                Arrays.asList("B")
        );

        TableHeader sortHeader = LogicalSortOperator.computeSortHeader(header, headerA);
        Operator sort = new InMemorySortOperator(opA, sortHeader);

        assertEquals(Arrays.asList(2, 1, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(5, 1, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(3, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(4, 3, 2), sort.getNextTuple().fields);
    }

    @Test
    public void testMultipleColumn() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("C", "B")
        );

        TableHeader sortHeader = LogicalSortOperator.computeSortHeader(header, headerA);
        Operator sort = new InMemorySortOperator(opA, sortHeader);

        assertEquals(Arrays.asList(5, 1, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(3, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(4, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(2, 1, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 3), sort.getNextTuple().fields);
    }

    @Test
    public void seek() throws Exception {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("C", "B")
        );

        SeekableOperator sort = new InMemorySortOperator(opA, header);

        List<Tuple> tuples = new ArrayList<>();
        while (sort.hasNextTuple()) {
            tuples.add(sort.getNextTuple());
        }

        for (int i = 0; i < 1000; i++) {
            int index = (int) (Math.random() * tuples.size());
            sort.seek(index);
            assertThat("Peek tuple " + index, sort.peekNextTuple(), equalTo(tuples.get(index)));
            assertThat("Next tuple " + index, sort.getNextTuple(), equalTo(tuples.get(index)));
        }
    }
}