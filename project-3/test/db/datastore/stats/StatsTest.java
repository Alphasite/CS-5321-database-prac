package db.datastore.stats;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.physical.physical.ScanOperator;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import static org.junit.Assert.assertEquals;

public class StatsTest {

    Database DB;

    TableInfo sailors;
    TableInfo boats;

    @Before
    public void loadData() {
        DB = Database.loadDatabase(TestUtils.NEW_DB_PATH);

        sailors = DB.getTable("Sailors");
        boats = DB.getTable("Boats");
    }

    @Test
    public void testStatsCollection() {
        TableStats sailorsStats = StatsGatherer.gatherStats(sailors);

        try {
            new ScanOperator(sailors).dump(new StringTupleWriter(new FileOutputStream("tuples.csv")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        assertEquals(10_000, sailorsStats.count);
        assertEquals(1, sailorsStats.minimums[0]);
        assertEquals(0, sailorsStats.minimums[1]);
        assertEquals(2, sailorsStats.minimums[2]);
        assertEquals(10_000, sailorsStats.maximums[0]);
        assertEquals(10_000, sailorsStats.maximums[1]);
        assertEquals(9_998, sailorsStats.maximums[2]);

        TableStats boatsStats = StatsGatherer.gatherStats(boats);

        assertEquals(10_000, boatsStats.count);
        assertEquals(1, boatsStats.minimums[0]);
        assertEquals(4, boatsStats.minimums[1]);
        assertEquals(0, boatsStats.minimums[2]);
        assertEquals(10_000, boatsStats.maximums[0]);
        assertEquals(9_999, boatsStats.maximums[1]);
        assertEquals(10_000, boatsStats.maximums[2]);
    }
}
