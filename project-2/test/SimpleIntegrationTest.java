import datastore.Database;
import datastore.Table;
import datastore.Tuple;
import operators.bag.Join;
import operators.physcial.Scan;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SimpleIntegrationTest {
    @Test
    public void simpleIntegration() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Optional<Database> databaseOptional = Database.loadDatabase(inputDir);

        Database database = databaseOptional.get();

        Optional<Table> boats = database.getTable("Boats");
        Optional<Table> reserves = database.getTable("Reserves");

        Optional<Scan> boatsScan = Scan.setupScan(boats.get());
        Optional<Scan> reservesScan = Scan.setupScan(reserves.get());

        Join join = new Join(boatsScan.get(), reservesScan.get());

        boolean printedHeader = false;
        Optional<Tuple> row;

        int i = 0;
        while ((row = join.getNextTuple()).isPresent()) {
            if (!printedHeader) {
                System.out.println(row.get().header.toHeaderForm());
                printedHeader = true;
            }

            System.out.println(i++ + ": " + row.get().toRowForm());
        }
    }
}
