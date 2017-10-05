package db;

import db.datastore.Database;
import db.operators.physcial.Operator;
import db.operators.physcial.bag.JoinOperator;
import db.operators.physcial.bag.ProjectionOperator;
import db.operators.physcial.bag.SelectionOperator;
import db.operators.physcial.extended.DistinctOperator;
import db.operators.physcial.extended.SortOperator;
import db.operators.physcial.physical.ScanOperator;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.BeforeClass;
import org.junit.Test;
import db.query.QueryBuilder;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Various test cases to check the operator tree created by the db.query builder
 */
public class QueryBuilderTest {
    private static Database DB;

    @BeforeClass
    public static void loadData() {
        DB = Database.loadDatabase(Paths.get(Project3.DB_PATH));
    }

    @Test
    public void testSimpleScan() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof ScanOperator);
    }

    @Test
    public void testSimpleSelection() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Boats WHERE Boats.D = 4;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof SelectionOperator);
        SelectionOperator select = (SelectionOperator) root;
        assertEquals(tokens.getWhere(), select.getPredicate());
    }

    @Test
    public void testSimpleProjection() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT Sailors.A FROM Sailors;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals(1, root.getHeader().size());
        assertEquals("Sailors", root.getHeader().columnAliases.get(0));
        assertEquals("A", root.getHeader().columnHeaders.get(0));
    }

    @Test
    public void testConditionalJoin() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof JoinOperator);
        JoinOperator join = (JoinOperator) root;
        assertEquals("Sailors.A | Sailors.B | Sailors.C | Reserves.G | Reserves.H", join.getHeader().toString());
        assertEquals(tokens.getWhere(), join.getPredicate());

        assertEquals("64 | 113 | 139 | 64 | 156", root.getNextTuple().toString());
        assertEquals("64 | 113 | 139 | 64 | 70", root.getNextTuple().toString());
    }

    @Test
    public void testAliases() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT S.C FROM Sailors S;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals("S", root.getHeader().columnAliases.get(0));
        assertEquals("C", root.getHeader().columnHeaders.get(0));

        assertEquals("139", root.getNextTuple().toString());
        assertEquals("129", root.getNextTuple().toString());
        assertEquals("118", root.getNextTuple().toString());
    }

    @Test
    public void testOrderBy() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.C;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof SortOperator);

        assertEquals("4 | 77 | 1", root.getNextTuple().toString());
        assertEquals("111 | 42 | 1", root.getNextTuple().toString());
        assertEquals("123 | 116 | 1", root.getNextTuple().toString());
        assertEquals("141 | 61 | 1", root.getNextTuple().toString());

        tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.B, S.C;");
        root = builder.buildQuery(tokens);

        assertEquals("130 | 0 | 7", root.getNextTuple().toString());
        assertEquals("109 | 0 | 24", root.getNextTuple().toString());
        assertEquals("192 | 0 | 29", root.getNextTuple().toString());
        assertEquals("156 | 0 | 64", root.getNextTuple().toString());
    }

    @Test
    public void testDistinct() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT DISTINCT Sailors.B FROM Sailors;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof DistinctOperator);

        assertEquals("0", root.getNextTuple().toString());
        assertEquals("1", root.getNextTuple().toString());
        assertEquals("2", root.getNextTuple().toString());
    }
}
