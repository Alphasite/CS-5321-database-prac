import datastore.Database;
import datastore.TableHeader;
import datastore.TableInfo;
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
    TableInfo boats;
    TableInfo reserves;
    TableInfo sailors;

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
        System.out.println("Simple Integration");

        Optional<Scan> boatsScan = Scan.setupScan(boats);
        Optional<Scan> reservesScan = Scan.setupScan(reserves);

        Join join = new Join(boatsScan.get(), reservesScan.get());

        assertThat(join.dump(System.out), is(5 * 6));
    }

    @Test
    public void projectionTest() throws Exception {
        System.out.println("Projection Test");

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

        assertThat(projection.dump(System.out), is(5 * 6));
    }
}
