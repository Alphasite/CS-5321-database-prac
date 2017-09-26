import datastore.Database;
import datastore.Table;
import datastore.TableHeader;
import datastore.Tuple;
import operators.bag.Join;
import operators.bag.Projection;
import operators.physical.Scan;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleIntegrationTest {
    Database database;
    Table boats;
    Table reserves;
    Table sailors;

    @Before
    public void setUp() throws Exception {
        Path inputDir = Paths.get("resources/samples/input/db");
        inputDir = inputDir.toAbsolutePath();
        Optional<Database> databaseOptional = Database.loadDatabase(inputDir);

        database = databaseOptional.get();

        this.boats = database.getTable("Boats").get();
        this.reserves = database.getTable("Reserves").get();
        this.sailors = database.getTable("Sailors").get();
    }

    @Test
    public void simpleIntegration() throws Exception {
        Optional<Scan> boatsScan = Scan.setupScan(boats);
        Optional<Scan> reservesScan = Scan.setupScan(reserves);

        Join join = new Join(boatsScan.get(), reservesScan.get());

        join.dump(System.out);
        Optional<Tuple> row;

        System.out.println(join.getHeader());

        int i = 0;
        while ((row = join.getNextTuple()).isPresent()) {

            System.out.println(++i + ": " + row.get());
        }

        assertThat(i, is(5 * 6));
    }

    @Test
    public void projectionTest() throws Exception {
        Optional<Scan> boatsScan = Scan.setupScan(boats);
        Optional<Scan> reservesScan = Scan.setupScan(reserves);

        TableHeader tableHeader = new TableHeader(
                new ArrayList<>(),
                new ArrayList<>()
        );

        tableHeader.columnAliases.add("Boats");
        tableHeader.columnHeaders.add("D");
        tableHeader.columnAliases.add("");
        tableHeader.columnHeaders.add("E");
        tableHeader.columnAliases.add("Reserves");
        tableHeader.columnHeaders.add("G");

        Join join = new Join(boatsScan.get(), reservesScan.get());
        Projection projection = new Projection(tableHeader, join);

        Optional<Tuple> row;

        System.out.println(projection.getHeader());

        int i = 0;
        while ((row = projection.getNextTuple()).isPresent()) {
            System.out.println(++i + ": " + row.get());
        }

        assertThat(i, is(5 * 6));
    }
}
