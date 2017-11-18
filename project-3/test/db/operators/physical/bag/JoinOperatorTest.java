package db.operators.physical.bag;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.physical.Operator;
import db.operators.physical.physical.ScanOperator;
import db.query.TriFunction;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class JoinOperatorTest {
    private List<Tuple> tuplesA;
    private DummyOperator opA;

    private List<Tuple> tuplesB;
    private DummyOperator opB;

    private TriFunction<Operator, Operator, Expression, JoinOperator> joinFactory;

    TableInfo table1;
    TableInfo table2;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> operators = new ArrayList<>();

        TriFunction<Operator, Operator, Expression, JoinOperator> tuple = TupleNestedJoinOperator::new;
        TriFunction<Operator, Operator, Expression, JoinOperator> block = BlockNestedJoinOperator::new;

        operators.add(new Object[]{"Tuple Nested Loop Join", tuple});
        operators.add(new Object[]{"Block Nested Loop Join", block});

        return operators;
    }

    public JoinOperatorTest(String name, TriFunction<Operator, Operator, Expression, JoinOperator> joinFactory) {
        this.joinFactory = joinFactory;
    }

    @Before
    public void loadData() {
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

        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        this.table1 = database.getTable("Sailors");
        this.table2 = database.getTable("Reserves");
    }

    @Test
    public void testSchema() {
        List<String> expectedTables = Arrays.asList("Sailors", "Sailors", "Sailors", "Reserves", "Reserves");
        List<String> expectedColumns = Arrays.asList("A", "B", "C", "G", "H");

        JoinOperator join = this.joinFactory.apply(opA, opB, null);
        TableHeader result = join.getHeader();

        assertEquals(expectedTables, result.tableIdentifiers);
        assertEquals(expectedColumns, result.columnNames);
    }

    @Test
    public void testJoin() {
        JoinOperator join = this.joinFactory.apply(opA, opB, null);
        Set<Tuple> tuples = new HashSet<>();

        int count = 0;
        Tuple next;
        while ((next = join.getNextTuple()) != null) {
            tuples.add(next);
            count += 1;
        }

        assertEquals(tuplesA.size() * tuplesB.size(), count);
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(1, 200, 50, 1, 101))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(1, 200, 50, 2, 101))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(2, 200, 200, 1, 101))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(4, 100, 50, 3, 102))));
    }

    @Test
    public void testFiltering() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");
        JoinOperator join = joinFactory.apply(opA, opB, tokens.getWhere());

        List<Tuple> tuples = new ArrayList<>();
        Tuple next;

        while ((next = join.getNextTuple()) != null) {
            tuples.add(next);
        }

        assertEquals(6, tuples.size());
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(1, 200, 50, 1, 101))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(1, 200, 50, 1, 103))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(2, 200, 200, 2, 101))));
        assertEquals(true, tuples.contains(new Tuple(Arrays.asList(4, 100, 50, 4, 104))));
    }

    @Test
    public void large() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");
        JoinOperator join = joinFactory.apply(new ScanOperator(table1), new ScanOperator(table2), tokens.getWhere());

        int i = 0;
        while (join.hasNextTuple()) {
            Tuple tuple = join.getNextTuple();
            assertThat(tuple, notNullValue());
            assertEquals(tuple.fields.get(0), tuple.fields.get(3));
            i++;
        }

        assertThat(i, not(equalTo((0))));
    }
}
