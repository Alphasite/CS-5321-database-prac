package db.operators.bag;

import db.TestUtils;
import db.datastore.TableHeader;
import db.datastore.Tuple;
import db.operators.DummyOperator;
import db.operators.Operator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RenameOperatorTest {
    private List<Tuple> tuplesA;
    private DummyOperator opA;
    private TableHeader headerA;

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
    }

    @Test
    public void testSchema() {
        List<String> expectedTables = Arrays.asList("BANANAS", "BANANAS", "BANANAS");
        List<String> expectedColumns = Arrays.asList("A", "B", "C");

        Operator op = new RenameOperator(opA, "BANANAS");
        TableHeader result = op.getHeader();

        assertEquals(expectedTables, result.columnAliases);
        assertEquals(expectedColumns, result.columnHeaders);
    }

    @Test
    public void testRename() throws Exception {
        Operator op = new RenameOperator(opA, "BANANAS");
        TestUtils.compareTuples(op, new DummyOperator(tuplesA, headerA));
    }
}