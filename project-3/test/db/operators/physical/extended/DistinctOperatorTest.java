package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.physical.Operator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DistinctOperatorTest {
    private DummyOperator opA;

    private DummyOperator opB;

    @Before
    public void setUp() {
        List<Tuple> tuplesA = new ArrayList<>();
        tuplesA.add(new Tuple(Arrays.asList(100)));
        tuplesA.add(new Tuple(Arrays.asList(100)));
        tuplesA.add(new Tuple(Arrays.asList(100)));
        tuplesA.add(new Tuple(Arrays.asList(200)));
        tuplesA.add(new Tuple(Arrays.asList(200)));
        tuplesA.add(new Tuple(Arrays.asList(300)));
        TableHeader headerA = new TableHeader(Arrays.asList("Sailors"), Arrays.asList("B"));
        opA = new DummyOperator(tuplesA, headerA);

        List<Tuple> tuplesB = new ArrayList<>();
        tuplesB.add(new Tuple(Arrays.asList(100, 50)));
        tuplesB.add(new Tuple(Arrays.asList(100, 100)));
        tuplesB.add(new Tuple(Arrays.asList(100, 100)));
        tuplesB.add(new Tuple(Arrays.asList(200, 50)));
        tuplesB.add(new Tuple(Arrays.asList(200, 50)));
        tuplesB.add(new Tuple(Arrays.asList(200, 300)));
        tuplesB.add(new Tuple(Arrays.asList(300, 100)));
        tuplesB.add(new Tuple(Arrays.asList(300, 300)));
        TableHeader headerB = new TableHeader(Arrays.asList("Sailors", "Sailors"), Arrays.asList("B", "C"));
        opB = new DummyOperator(tuplesB, headerB);
    }

    @Test
    public void testSimpleDistinct() {
        Operator distinct = new DistinctOperator(opA);

        assertEquals(Arrays.asList(100), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(200), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(300), distinct.getNextTuple().fields);
    }

    @Test
    public void testMultipleDistinct() {
        Operator distinct = new DistinctOperator(opB);

        assertEquals(Arrays.asList(100, 50), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(100, 100), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(200, 50), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(200, 300), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(300, 100), distinct.getNextTuple().fields);
        assertEquals(Arrays.asList(300, 300), distinct.getNextTuple().fields);
    }
}