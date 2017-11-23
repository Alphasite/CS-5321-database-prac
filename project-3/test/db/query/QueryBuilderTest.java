package db.query;

import db.TestUtils;
import db.Utilities.UnionFind;
import db.datastore.Database;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.operators.physical.bag.JoinOperator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.SelectionOperator;
import db.operators.physical.bag.TupleNestedJoinOperator;
import db.operators.physical.extended.DistinctOperator;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.physical.ScanOperator;
import db.query.visitors.PhysicalPlanBuilder;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Various test cases to check the operator tree created by the db.query builder
 *
 * TODO: probably change these tests to match new functionality
 */
public class QueryBuilderTest {
    private static Database DB;

    private QueryBuilder logicalBuilder;
    private PhysicalPlanBuilder physicalBuilder;

    @BeforeClass
    public static void loadData() {
        DB = Database.loadDatabase(TestUtils.DB_PATH);
    }

    @Before
    public void init() {
        this.logicalBuilder = new QueryBuilder(DB);
        this.physicalBuilder = new PhysicalPlanBuilder(TestUtils.TEMP_PATH, TestUtils.DB_PATH.resolve("indexes"));
    }

    @Test
    public void testSimpleScan() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertTrue(((ProjectionOperator) root).getChild() instanceof ScanOperator);

        root.close();
    }

    @Test
    public void testSimpleSelection() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertTrue(((ProjectionOperator) root).getChild() instanceof SelectionOperator);
        SelectionOperator select = (SelectionOperator) ((ProjectionOperator) root).getChild();
        assertEquals("Boats.D >= 4 AND Boats.D <= 4", select.getPredicate().toString());

        root.close();
    }

    @Test
    public void testSimpleProjection() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT Sailors.A FROM Sailors;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals(1, root.getHeader().size());
        assertEquals("Sailors", root.getHeader().tableIdentifiers.get(0));
        assertEquals("A", root.getHeader().columnNames.get(0));

        root.close();
    }

    @Test
    public void testConditionalJoin() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertTrue(((ProjectionOperator) root).getChild() instanceof TupleNestedJoinOperator);
        JoinOperator operator = (JoinOperator) ((ProjectionOperator) root).getChild();
        assertEquals("Sailors.A | Sailors.B | Sailors.C | Reserves.G | Reserves.H", operator.getHeader().toString());

        assertEquals("64 | 113 | 139 | 64 | 156", root.getNextTuple().toString());
        assertEquals("64 | 113 | 139 | 64 | 70", root.getNextTuple().toString());

        root.close();
    }

    @Test
    public void testNakedConditions() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors WHERE 4 < 8 AND 6 >= 3;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        int count = 0;
        while (root.getNextTuple() != null) {
            count++;
        }

        assertEquals(1000, count);

        init();

        tokens = TestUtils.parseQuery("SELECT * FROM Sailors WHERE 6 <= 8 AND 1 > 5;");

        logRoot = logicalBuilder.buildQuery(tokens);
        root = physicalBuilder.buildFromLogicalTree(logRoot);

        count = 0;
        while (root.getNextTuple() != null) {
            count++;
        }

        assertEquals(0, count);
    }

    @Test
    public void testAliases() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT S.C FROM Sailors S;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals("S", root.getHeader().tableIdentifiers.get(0));
        assertEquals("C", root.getHeader().columnNames.get(0));

        assertEquals("139", root.getNextTuple().toString());
        assertEquals("129", root.getNextTuple().toString());
        assertEquals("118", root.getNextTuple().toString());

        root.close();
    }

    @Test
    public void testOrderBy() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.C;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof SortOperator);

        assertEquals("4 | 77 | 1", root.getNextTuple().toString());
        assertEquals("111 | 42 | 1", root.getNextTuple().toString());
        assertEquals("123 | 116 | 1", root.getNextTuple().toString());
        assertEquals("141 | 61 | 1", root.getNextTuple().toString());

        tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.B, S.C;");
        logRoot = logicalBuilder.buildQuery(tokens);
        root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertEquals("130 | 0 | 7", root.getNextTuple().toString());
        assertEquals("109 | 0 | 24", root.getNextTuple().toString());
        assertEquals("192 | 0 | 29", root.getNextTuple().toString());
        assertEquals("156 | 0 | 64", root.getNextTuple().toString());

        root.close();
    }

    @Test
    public void testDistinct() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT DISTINCT Sailors.B FROM Sailors;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof DistinctOperator);

        assertEquals("0", root.getNextTuple().toString());
        assertEquals("1", root.getNextTuple().toString());
        assertEquals("2", root.getNextTuple().toString());

        root.close();
    }

    @Test
    public void testUnionFind() throws Exception {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Boats WHERE Boats.D < 4 AND Boats.D = Sailors.A AND Sailors.A = Boats.E And Sailors.B = Boats.F And Boats.F > 6 AND Boats.F <= 95;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);

        UnionFind unionFind = logicalBuilder.getUnionFind();

        List<Set<String>> sets = unionFind.getSets();

        assertEquals(3, sets.size());

        Set<String> set1 = sets.stream()
                .filter(set -> set.size() == 1)
                .findAny()
                .get();

        Set<String> set2 = sets.stream()
                .filter(set -> set.size() == 2)
                .findAny()
                .get();

        Set<String> set3 = sets.stream()
                .filter(set -> set.size() == 3)
                .findAny()
                .get();

        assertThat(set1, hasItems("Sailors.C"));
        assertThat(set2, hasItems("Boats.F", "Sailors.B"));
        assertThat(set3, hasItems("Boats.D", "Sailors.A", "Boats.E"));

        assertThat(unionFind.getMaximum("Sailors.C"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.C"), nullValue());
        assertThat(unionFind.getEquals("Sailors.C"), nullValue());

        assertThat(unionFind.getMaximum("Boats.D"), is(3));
        assertThat(unionFind.getMaximum("Boats.E"), is(3));
        assertThat(unionFind.getMaximum("Sailors.A"), is(3));
        assertThat(unionFind.getMinimum("Boats.D"), nullValue());
        assertThat(unionFind.getMinimum("Boats.E"), nullValue());
        assertThat(unionFind.getMinimum("Sailors.A"), nullValue());
        assertThat(unionFind.getEquals("Boats.D"), nullValue());
        assertThat(unionFind.getEquals("Boats.E"), nullValue());
        assertThat(unionFind.getEquals("Sailors.A"), nullValue());

        assertThat(unionFind.getMaximum("Boats.F"), is(95));
        assertThat(unionFind.getMaximum("Sailors.B"), is(95));
        assertThat(unionFind.getMinimum("Boats.F"), is(7));
        assertThat(unionFind.getMinimum("Sailors.B"), is(7));
        assertThat(unionFind.getEquals("Boats.F"), nullValue());
        assertThat(unionFind.getEquals("Sailors.B"), nullValue());
    }
}
