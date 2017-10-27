package db.operators.physical.extended;

import db.Project3;
import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.logical.LogicalSortOperator;
import db.operators.physical.Operator;
import db.operators.physical.SeekableOperator;
import db.operators.physical.bag.TupleNestedJoinOperator;
import db.operators.physical.physical.ScanOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;

/**
 * Test reordering of tuples by the sort operator
 * Pay particular attention to conflict resolution as explained in section 2.1
 */
public class ExternalSortOperatorTest {
    private List<Tuple> tuplesA;
    private TableHeader headerA;
    private DummyOperator opA;

    @Before
    public void setUp() {
        tuplesA = new ArrayList<>();
        tuplesA.add(new Tuple(Arrays.asList(1, 2, 1)));
        tuplesA.add(new Tuple(Arrays.asList(1, 2, 3)));
        tuplesA.add(new Tuple(Arrays.asList(2, 1, 3)));
        tuplesA.add(new Tuple(Arrays.asList(3, 3, 2)));
        tuplesA.add(new Tuple(Arrays.asList(4, 3, 2)));
        tuplesA.add(new Tuple(Arrays.asList(5, 1, 1)));
        headerA = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));
        opA = new DummyOperator(tuplesA, headerA);
    }

    @Test
    public void testSingleColumn() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors"),
                Arrays.asList("B")
        );

        Operator sort = new ExternalSortOperator(opA, header, 3, Paths.get(Project3.TEMP_PATH));

        assertEquals(Arrays.asList(2, 1, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(5, 1, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(3, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(4, 3, 2), sort.getNextTuple().fields);

        sort.close();
    }

    @Test
    public void testMultipleColumn() {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("C", "B")
        );

        Operator sort = new ExternalSortOperator(opA, header, 3, Paths.get(Project3.TEMP_PATH));

        assertEquals(Arrays.asList(5, 1, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 1), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(3, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(4, 3, 2), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(2, 1, 3), sort.getNextTuple().fields);
        assertEquals(Arrays.asList(1, 2, 3), sort.getNextTuple().fields);

        sort.close();
    }

    @Test
    public void testMultiPageQuery() {
        Database DB = Database.loadDatabase(Paths.get(Project3.DB_PATH));

        ScanOperator S = new ScanOperator(DB.getTable("Sailors"));
        ScanOperator R = new ScanOperator(DB.getTable("Reserves"));
        ScanOperator B = new ScanOperator(DB.getTable("Boats"));

        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("C", "A")
        );

        Expression cmp1 = new EqualsTo(new Column(new Table(null, "Sailors"), "A"), new Column(new Table(null, "Reserves"), "G"));
        TupleNestedJoinOperator join = new TupleNestedJoinOperator(S, R, cmp1);

        Expression cmp2 = new EqualsTo(new Column(new Table(null, "Reserves"), "H"), new Column(new Table(null, "Boats"), "D"));
        join = new TupleNestedJoinOperator(join, B, cmp2);

        TableHeader sortHeaders = LogicalSortOperator.computeSortHeader(header, join.getHeader());
        Operator sort = new ExternalSortOperator(join, sortHeaders, 10, Paths.get(Project3.TEMP_PATH));

        assertEquals(25224, TestUtils.countNotNullTuples(sort));

        sort.reset();

        Path sampleFile = Paths.get("resources/samples/expected").resolve("query14_humanreadable");
        Operator sampleOp = new ScanOperator(new TableInfo(sort.getHeader(), sampleFile, false));

        TestUtils.compareTuples(sampleOp, sort);

        sort.close();
        sampleOp.close();
    }

    @Test
    public void seek() throws Exception {
        TableHeader header = new TableHeader(
                Arrays.asList("Sailors", "Sailors"),
                Arrays.asList("C", "B")
        );

        Database DB = Database.loadDatabase(Paths.get(Project3.DB_PATH));

        ScanOperator S = new ScanOperator(DB.getTable("Sailors"));

        SeekableOperator sort = new ExternalSortOperator(S, header, 3, Files.createTempDirectory("test"));

        List<Tuple> tuples = new ArrayList<>();
        while (sort.hasNextTuple()) {
            tuples.add(sort.getNextTuple());
        }

        for (int i = 0; i < tuples.size(); i++) {
            sort.seek(i);
            assertThat("Peek tuple " + i, sort.peekNextTuple(), equalTo(tuples.get(i)));
            assertThat("Next tuple " + i, sort.getNextTuple(), equalTo(tuples.get(i)));
        }

        sort.seek(tuples.size() - 1);
        assertThat(sort.getNextTuple(), equalTo(tuples.get(tuples.size() - 1)));
        assertThat(sort.peekNextTuple(), is(nullValue()));
        sort.seek(0);
        assertThat(sort.getNextTuple(), equalTo(tuples.get(0)));
        assertThat(sort.peekNextTuple(), is(notNullValue()));
        sort.seek(10);
        assertThat(sort.getNextTuple(), equalTo(tuples.get(10)));
        assertThat(sort.peekNextTuple(), is(notNullValue()));
        sort.seek(999);
        assertThat(sort.getNextTuple(), equalTo(tuples.get(999)));
        assertThat(sort.peekNextTuple(), is(nullValue()));

        for (int i = 0; i < 1000; i++) {
            int index = (int) (Math.random() * tuples.size());
            sort.seek(index);
            assertThat("Peek tuple " + index, sort.peekNextTuple(), equalTo(tuples.get(index)));
            assertThat("Next tuple " + index, sort.getNextTuple(), equalTo(tuples.get(index)));
        }

        S.close();
    }

    @After
    public void cleanup() {
//        Utilities.cleanDirectory(Paths.get(Project3.TEMP_PATH));
    }
}
