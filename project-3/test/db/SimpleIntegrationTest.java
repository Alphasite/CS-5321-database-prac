package db;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.operators.physical.bag.JoinOperator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.TupleNestedJoinOperator;
import db.operators.physical.physical.ScanOperator;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimpleIntegrationTest {
    private TableInfo boats;
    private TableInfo reserves;
    private TableInfo sailors;

    @Before
    public void setUp() throws Exception {
        Database database = Database.loadDatabase(TestUtils.DB_PATH);
        this.boats = database.getTable("Boats");
        this.reserves = database.getTable("Reserves");
        this.sailors = database.getTable("Sailors");
    }

    @Test
    public void simpleIntegration() throws Exception {
        ScanOperator boatsScan = new ScanOperator(boats);
        ScanOperator reservesScan = new ScanOperator(reserves);

        JoinOperator join = new TupleNestedJoinOperator(boatsScan, reservesScan);
        assertThat(TestUtils.countNotNullTuples(join), is(1000 * 1000));
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

        JoinOperator join = new TupleNestedJoinOperator(boatsScan, reservesScan);
        ProjectionOperator projection = new ProjectionOperator(join, tableHeader);

        assertThat(TestUtils.countNotNullTuples(projection), is(1000 * 1000));
    }
}
