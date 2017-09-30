import datastore.Database;
import datastore.TableInfo;
import operators.physical.Scan;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ScanTest {

    @Test
    public void testScan () {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Database database = Database.loadDatabase(inputDir);
        TableInfo sailors;
        sailors = database.getTable("Sailors");
        Scan scan = new Scan(sailors);

        List<Integer> tuple1 = new ArrayList<>();
        tuple1.add(1);
        tuple1.add(200);
        tuple1.add(50);
        assertTrue(tuple1.equals(scan.getNextTuple().fields));
    }



}
