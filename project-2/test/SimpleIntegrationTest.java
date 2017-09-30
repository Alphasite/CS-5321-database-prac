import datastore.Database;
import datastore.TableHeader;
import datastore.TableInfo;
import datastore.Tuple;
import operators.bag.Join;
import operators.bag.Projection;
import operators.physical.Scan;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

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
        Database database = Database.loadDatabase(inputDir);
        this.boats = database.getTable("Boats");
        this.reserves = database.getTable("Reserves");
        this.sailors = database.getTable("Sailors");
    }

    @Test
    public void simpleIntegration() throws Exception {
        Scan boatsScan = new Scan(boats);
        Scan reservesScan = new Scan(reserves);

        Join join = new Join(boatsScan, reservesScan);

        join.dump(System.out);
        Tuple row;

        System.out.println(join.getHeader());

        int i = 0;
        while ((row = join.getNextTuple())!=null) {

            System.out.println(++i + ": " + row);
        }

        assertThat(i, is(5 * 6));
    }

    @Test
    public void projectionTest() throws Exception {
        Scan boatsScan = new Scan(boats);
        Scan reservesScan = new Scan(reserves);

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

        Join join = new Join(boatsScan, reservesScan);
        Projection projection = new Projection(tableHeader, join);

        Tuple row;

        System.out.println(projection.getHeader());

        int i = 0;
        while ((row = projection.getNextTuple())!=null) {
            System.out.println(++i + ": " + row);
        }

        assertThat(i, is(5 * 6));
    }
}
