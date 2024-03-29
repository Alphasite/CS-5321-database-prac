package db;

import db.datastore.Database;
import db.operators.Operator;
import db.operators.bag.JoinOperator;
import db.operators.bag.ProjectionOperator;
import db.operators.bag.SelectionOperator;
import db.operators.extended.DistinctOperator;
import db.operators.extended.SortOperator;
import db.operators.physical.ScanOperator;
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
        DB = Database.loadDatabase(Paths.get(Project2.DB_PATH));
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

        assertEquals("1 | 200 | 50 | 1 | 101", root.getNextTuple().toString());
        assertEquals("1 | 200 | 50 | 1 | 102", root.getNextTuple().toString());
    }

    @Test
    public void testAliases() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT S.C FROM Sailors S;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof ProjectionOperator);
        assertEquals("S", root.getHeader().columnAliases.get(0));
        assertEquals("C", root.getHeader().columnHeaders.get(0));

        assertEquals("50", root.getNextTuple().toString());
        assertEquals("200", root.getNextTuple().toString());
        assertEquals("105", root.getNextTuple().toString());
    }

    @Test
    public void testOrderBy() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.C;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof SortOperator);

        assertEquals("1 | 200 | 50", root.getNextTuple().toString());
        assertEquals("4 | 100 | 50", root.getNextTuple().toString());
        assertEquals("3 | 100 | 105", root.getNextTuple().toString());
        assertEquals("2 | 200 | 200", root.getNextTuple().toString());

        tokens = TestUtils.parseQuery("SELECT * FROM Sailors S ORDER BY S.B, S.C;");
        root = builder.buildQuery(tokens);

        assertEquals("4 | 100 | 50", root.getNextTuple().toString());
        assertEquals("3 | 100 | 105", root.getNextTuple().toString());
        assertEquals("5 | 100 | 500", root.getNextTuple().toString());
        assertEquals("1 | 200 | 50", root.getNextTuple().toString());
    }

    @Test
    public void testDistinct() {
        PlainSelect tokens = TestUtils.parseQuery("SELECT DISTINCT Sailors.B FROM Sailors;");
        QueryBuilder builder = new QueryBuilder(DB);

        Operator root = builder.buildQuery(tokens);

        assertTrue(root instanceof DistinctOperator);

        assertEquals("100", root.getNextTuple().toString());
        assertEquals("200", root.getNextTuple().toString());
        assertEquals("300", root.getNextTuple().toString());
    }
}
