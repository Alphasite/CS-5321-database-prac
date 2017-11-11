package db.datastore.stats;

import db.TestUtils;
import db.datastore.Database;
import db.datastore.TableInfo;
import org.junit.Before;
import org.junit.Test;

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

        assertEquals(10_000, sailorsStats.count);
        assertEquals(0, sailorsStats.minimums[0]);
        assertEquals(0, sailorsStats.minimums[1]);
        assertEquals(1, sailorsStats.minimums[2]);
        assertEquals(10_000, sailorsStats.maximums[0]);
        assertEquals(9_999, sailorsStats.maximums[0]);
        assertEquals(10_000, sailorsStats.maximums[0]);

        TableStats boatsStats = StatsGatherer.gatherStats(boats);

        assertEquals(10_000, boatsStats.count);
        assertEquals(1, boatsStats.minimums[0]);
        assertEquals(4, boatsStats.minimums[1]);
        assertEquals(0, boatsStats.minimums[2]);
        assertEquals(10_000, boatsStats.maximums[0]);
        assertEquals(9_999, boatsStats.maximums[0]);
        assertEquals(10_000, boatsStats.maximums[0]);
    }
}
