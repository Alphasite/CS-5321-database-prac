package db.operators.physical.physical;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ScanOperatorTest {

    @Test
    public void testScan() {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        TableInfo sailors;
        sailors = database.getTable("Sailors");
        ScanOperator scan = new ScanOperator(sailors);

        List<Integer> tuple1 = new ArrayList<>();
        tuple1.add(64);
        tuple1.add(113);
        tuple1.add(139);

        Tuple nextTuple = scan.getNextTuple();

        assertThat(nextTuple, notNullValue());
        assertThat(nextTuple.fields, equalTo(tuple1));

        scan.close();
    }

    @Test
    public void testSchema() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        TableInfo sailors;
        sailors = database.getTable("Sailors");
        ScanOperator scan = new ScanOperator(sailors);

        TableHeader header = new TableHeader(Arrays.asList("Sailors", "Sailors", "Sailors"), Arrays.asList("A", "B", "C"));

        assertEquals(scan.getHeader().toString(), header.toString());

        scan.close();
    }
}
