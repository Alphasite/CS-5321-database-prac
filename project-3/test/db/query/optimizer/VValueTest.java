package db.query.optimizer;

import db.TestUtils;
import db.Utilities.UnionFind;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.operators.logical.LogicalScanOperator;
import db.query.visitors.WhereDecomposer;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VValueTest {

    private static Database DB;

    private VValues vBoats;
    private VValues vSailors;

    @BeforeClass
    public static void loadData() {
        DB = Database.loadDatabase(TestUtils.NEW_DB_PATH);
    }

    @Before
    public void init() {
        TableInfo boats = DB.getTable("Boats");
        TableInfo sailors = DB.getTable("Sailors");

        vBoats = new VValues(new LogicalScanOperator(boats, "B"));
        vSailors = new VValues(new LogicalScanOperator(sailors, "S"));
    }

    @Test
    public void testBaseTableVValues() {
        assertEquals(10_000, vBoats.getAttributeVValue("B.D"));
        assertEquals(9_996, vBoats.getAttributeVValue("B.E"));
        assertEquals(10_000, vBoats.getAttributeVValue("B.F"));

        assertEquals(10_000, vSailors.getAttributeVValue("S.A"));
        assertEquals(10_000, vSailors.getAttributeVValue("S.B"));
        assertEquals(9_997, vSailors.getAttributeVValue("S.C"));
    }

    @Test
    public void testConstrainedVValues() {
        String query = "SELECT * FROM Sailors S, Boats B WHERE S.A <= 5000 AND B.E = S.A AND S.C > 100 AND B.F = S.C";
        PlainSelect tokens = TestUtils.parseQuery(query);

        WhereDecomposer bwb = new WhereDecomposer();
        tokens.getWhere().accept(bwb);

        UnionFind unionFind = bwb.getUnionFind();

        vBoats.updateConstraints(unionFind);
        vSailors.updateConstraints(unionFind);

        assertEquals(5_000, vSailors.getAttributeVValue("S.A"));
        assertEquals(10_000, vSailors.getAttributeVValue("S.B"));
        assertEquals(9_898, vSailors.getAttributeVValue("S.C"));

        assertEquals(10_000, vBoats.getAttributeVValue("B.D"));
        assertEquals(4_997, vBoats.getAttributeVValue("B.E"));
        assertEquals(9_900, vBoats.getAttributeVValue("B.F"));
    }
}
