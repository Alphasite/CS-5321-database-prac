package db.operators.bag;

import db.TestUtils;
import db.datastore.TableHeader;
import db.datastore.Tuple;
import db.operators.DummyOperator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProjectionOperatorTest {
    private List<Tuple> tuplesA;
    private List<Tuple> tuplesB;

    private TableHeader headerA;
    private TableHeader headerB;

    private DummyOperator opA;
    private DummyOperator opB;


    @Before
    public void setUp() {
        tuplesA = new ArrayList<>();
        tuplesA.add(new Tuple(Arrays.asList(1, 200, 50)));
        tuplesA.add(new Tuple(Arrays.asList(2, 200, 200)));
        tuplesA.add(new Tuple(Arrays.asList(3, 100, 105)));
        tuplesA.add(new Tuple(Arrays.asList(4, 100, 50)));
        tuplesA.add(new Tuple(Arrays.asList(5, 100, 500)));
        tuplesA.add(new Tuple(Arrays.asList(6, 300, 400)));
        headerA = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        opA = new DummyOperator(tuplesA, headerA);

        tuplesB = new ArrayList<>();
        tuplesB.add(new Tuple(Arrays.asList(1, 200, 50)));
        tuplesB.add(new Tuple(Arrays.asList(2, 200, 200)));
        tuplesB.add(new Tuple(Arrays.asList(3, 100, 105)));
        tuplesB.add(new Tuple(Arrays.asList(4, 100, 50)));
        tuplesB.add(new Tuple(Arrays.asList(5, 100, 500)));
        tuplesB.add(new Tuple(Arrays.asList(6, 300, 400)));
        headerB = new TableHeader(Arrays.asList("Sailors", "Sailors"), Arrays.asList("A", "C"));
        opB = new DummyOperator(tuplesB, headerB);
    }

    @Test
    public void testSchema() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("A", "C")
        );

        ProjectionOperator projectionOperator = new ProjectionOperator(header, opA);
        assertEquals(projectionOperator.getHeader().toString(), opB.getHeader().toString());
    }

    @Test
    public void testProjection() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("A", "C")
        );

        ProjectionOperator projectionOperator = new ProjectionOperator(header, opA);

        TestUtils.compareTuples(opA, opB);
    }
}
