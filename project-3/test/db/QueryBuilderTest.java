package db;

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
import db.query.QueryBuilder;
import db.query.visitors.PhysicalPlanBuilder;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
        this.physicalBuilder = new PhysicalPlanBuilder(TestUtils.TEMP_PATH);
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
        assertEquals(tokens.getWhere(), select.getPredicate());

        root.close();
    }

    @Test
    public void testSimpleProjection() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT Sailors.A FROM Sailors;");

        LogicalOperator logRoot = logicalBuilder.buildQuery(tokens);
        Operator root = physicalBuilder.buildFromLogicalTree(logRoot);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals(1, root.getHeader().size());
        assertEquals("Sailors", root.getHeader().columnAliases.get(0));
        assertEquals("A", root.getHeader().columnHeaders.get(0));

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
        assertEquals(tokens.getWhere(), operator.getPredicate());

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
        assertEquals("S", root.getHeader().columnAliases.get(0));
        assertEquals("C", root.getHeader().columnHeaders.get(0));

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
}
