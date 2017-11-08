package db.operators.physical.physical;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.index.BTree;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNull.notNullValue;

@SuppressWarnings("Duplicates")
public class IndexScanOperatorTest {
    private final Path indexFile = Paths.get("resources/samples-4/expected_indexes/Boats.E").toAbsolutePath();
    private final Path inputDir = Paths.get("resources/samples-4/input/db").toAbsolutePath();

    private BTree indexTree;
    private TableInfo table;
    private IndexScanOperator operator;
    private int numberOfTuples;

    private int min = 42;
    private int max = 9994;

    @Before
    public void setUp() throws Exception {
        Database database = Database.loadDatabase(inputDir);
        table = database.getTable("Boats");
        indexTree = BTree.createTree(indexFile);
        operator = new IndexScanOperator(table, indexTree, min, max);
        numberOfTuples = 10000 - 4 - 32;
    }

    @After
    public void tearDown() throws Exception {
        operator.close();
    }

    @Test
    public void fullScan() throws Exception {
        operator.close();
        operator = new IndexScanOperator(table, indexTree, 0, 10000);

        TestUtils.unorderedCompareTuples(new ScanOperator(table), operator);

        while (operator.hasNextTuple()) {
            Tuple tuple = operator.getNextTuple();
            assertThat("not null", tuple, notNullValue());
            assertThat("class", tuple, instanceOf(Tuple.class));
            assertThat("size", tuple.fields.size(), is(3));
        }

        operator.close();
    }

    @Test
    public void number() throws Exception {
        while (operator.hasNextTuple()) {
            Tuple tuple = operator.getNextTuple();
            assertThat("not null", tuple, notNullValue());
            assertThat("class", tuple, instanceOf(Tuple.class));
            assertThat("size", tuple.fields.size(), is(3));
        }
    }

    @Test
    public void next() throws Exception {
        assertThat(operator, notNullValue());

        for (int i = 0; i < numberOfTuples; i++) {
            assertThat("tuple: " + i, operator.hasNextTuple(), equalTo(true));

            Tuple peek = operator.peekNextTuple();
            Tuple next = operator.getNextTuple();

            assertThat(peek, notNullValue());
            assertThat(peek, instanceOf(Tuple.class));
            assertThat(peek.fields.size(), equalTo(table.header.size()));

            assertThat(next, notNullValue());
            assertThat(next, instanceOf(Tuple.class));
            assertThat(next.fields.size(), equalTo(table.header.size()));

            assertThat(peek, equalTo(next));
        }

        assertThat(operator.hasNextTuple(), equalTo(false));
        assertThat(operator.peekNextTuple(), nullValue());
        assertThat(operator.getNextTuple(), nullValue());
        assertThat(operator.hasNextTuple(), equalTo(false));
    }

    @Test
    public void reset() throws Exception {
        List<Tuple> tuples = new ArrayList<>();

        while (operator.hasNextTuple()) {
            tuples.add(operator.getNextTuple());
        }

        assertThat(tuples.size(), is(numberOfTuples));

        for (int i = 0; i < 100; i++) {
            System.out.println("Iteration: " + i);

            operator.reset();
            TestUtils.compareTuples(new DummyOperator(tuples, operator.getHeader()), operator);
        }

        operator.close();
    }

    @Test
    public void getHeader() throws Exception {
        assertThat(operator.getHeader(), is(table.header));
    }
}