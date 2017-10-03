package db;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.operators.bag.JoinOperator;
import db.operators.bag.ProjectionOperator;
import db.operators.physical.ScanOperator;
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
        ScanOperator boatsScan = new ScanOperator(boats);
        ScanOperator reservesScan = new ScanOperator(reserves);

        JoinOperator join = new JoinOperator(boatsScan, reservesScan);
        assertThat(join.dump(System.out, false), is(5 * 6));
    }

    @Test
    public void projectionTest() throws Exception {
        ScanOperator boatsScan = new ScanOperator(boats);
        ScanOperator reservesScan = new ScanOperator(reserves);

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

        JoinOperator join = new JoinOperator(boatsScan, reservesScan);
        ProjectionOperator projection = new ProjectionOperator(tableHeader, join);

        assertThat(projection.dump(System.out, false), is(5 * 6));
    }
}
