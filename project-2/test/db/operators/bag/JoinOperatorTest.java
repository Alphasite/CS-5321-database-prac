package db.operators.bag;

import db.TestUtils;
import db.datastore.TableHeader;
import db.datastore.Tuple;
import db.operators.DummyOperator;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JoinOperatorTest {
    private static List<Tuple> tuplesA;
    private static DummyOperator opA;

    private static List<Tuple> tuplesB;
    private static DummyOperator opB;

    @BeforeClass
    public static void loadData() {
        tuplesA = new ArrayList<>();
        tuplesA.add(new Tuple(Arrays.asList(1, 200, 50)));
        tuplesA.add(new Tuple(Arrays.asList(2, 200, 200)));
        tuplesA.add(new Tuple(Arrays.asList(3, 100, 105)));
        tuplesA.add(new Tuple(Arrays.asList(4, 100, 50)));
        tuplesA.add(new Tuple(Arrays.asList(5, 100, 500)));
        tuplesA.add(new Tuple(Arrays.asList(6, 300, 400)));
        TableHeader headerA = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        opA = new DummyOperator(tuplesA, headerA);

        tuplesB = new ArrayList<>();
        tuplesB.add(new Tuple(Arrays.asList(1, 101)));
        tuplesB.add(new Tuple(Arrays.asList(1, 102)));
        tuplesB.add(new Tuple(Arrays.asList(1, 103)));
        tuplesB.add(new Tuple(Arrays.asList(2, 101)));
        tuplesB.add(new Tuple(Arrays.asList(3, 102)));
        tuplesB.add(new Tuple(Arrays.asList(4, 104)));
        TableHeader headerB = new TableHeader(Arrays.asList("Reserves", "Reserves"), Arrays.asList("G", "H"));
        opB = new DummyOperator(tuplesB, headerB);
    }

    @Test
    public void testSchema() {
        List<String> expectedTables = Arrays.asList("Sailors", "Sailors", "Sailors", "Reserves", "Reserves");
        List<String> expectedColumns = Arrays.asList("A", "B", "C", "G", "H");

        JoinOperator join = new JoinOperator(opA, opB);
        TableHeader result = join.getHeader();

        assertEquals(expectedTables, result.columnAliases);
        assertEquals(expectedColumns, result.columnHeaders);
    }

    @Test
    public void testJoin() {
        JoinOperator join = new JoinOperator(opA, opB);
        List<Tuple> tuples = new ArrayList<>();
        Tuple next;

        while ((next = join.getNextTuple()) != null) {
            tuples.add(next);
        }

        assertEquals(tuplesA.size() * tuplesB.size(), tuples.size());
        assertEquals(Arrays.asList(1, 200, 50, 1, 101), tuples.get(0).fields);
        assertEquals(Arrays.asList(1, 200, 50, 2, 101), tuples.get(3).fields);
        assertEquals(Arrays.asList(2, 200, 200, 1, 101), tuples.get(6).fields);
        assertEquals(Arrays.asList(4, 100, 50, 3, 102), tuples.get(22).fields);
    }

    @Test
    public void testFiltering() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");
        JoinOperator join = new JoinOperator(opA, opB, tokens.getWhere());

        List<Tuple> tuples = new ArrayList<>();
        Tuple next;

        while ((next = join.getNextTuple()) != null) {
            tuples.add(next);
        }

        assertEquals(6, tuples.size());
        assertEquals(Arrays.asList(1, 200, 50, 1, 101), tuples.get(0).fields);
        assertEquals(Arrays.asList(1, 200, 50, 1, 103), tuples.get(2).fields);
        assertEquals(Arrays.asList(2, 200, 200, 2, 101), tuples.get(3).fields);
        assertEquals(Arrays.asList(4, 100, 50, 4, 104), tuples.get(5).fields);
    }
}
