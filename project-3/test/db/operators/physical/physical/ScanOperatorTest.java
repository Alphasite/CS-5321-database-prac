package db.operators.physical.physical;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ScanOperatorTest {

    private TableInfo sailorsTable;

    @Before
    public void init() {
        Database DB = Database.loadDatabase(TestUtils.DB_PATH);
        sailorsTable = DB.getTable("Sailors");
    }

    @Test
    public void testScan() {
        ScanOperator scan = new ScanOperator(sailorsTable);

        List<Integer> tuple1 = Arrays.asList(64, 113, 139);
        List<Integer> tuple2 = Arrays.asList(181, 128, 129);
        List<Integer> tuple3 = Arrays.asList(147, 45, 118);

        Tuple nextTuple = scan.getNextTuple();

        assertThat(nextTuple, notNullValue());
        assertEquals(tuple1, nextTuple.fields);
        assertEquals(tuple2, scan.getNextTuple().fields);
        assertEquals(tuple3, scan.getNextTuple().fields);

        scan.close();
    }

    @Test
    public void testSchema() throws Exception {
        ScanOperator scan = new ScanOperator(sailorsTable);

        TableHeader header = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));

        assertEquals(scan.getHeader().toString(), header.toString());

        scan.close();
    }

    @Test
    public void testRename() {
        ScanOperator scan = new ScanOperator(sailorsTable, "S");

        TableHeader header = new TableHeader(Arrays.asList("S", "S", "S"), Arrays.asList("A", "B", "C"));

        assertEquals(scan.getHeader().toString(), header.toString());

        scan.close();
    }
}
