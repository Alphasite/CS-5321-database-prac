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
        TableStats sailorsStats = sailors.getStats();

        assertEquals(10_000, sailorsStats.count);
        assertEquals(1, sailorsStats.minimums[0]);
        assertEquals(0, sailorsStats.minimums[1]);
        assertEquals(2, sailorsStats.minimums[2]);
        assertEquals(10_000, sailorsStats.maximums[0]);
        assertEquals(10_000, sailorsStats.maximums[1]);
        assertEquals(9_998, sailorsStats.maximums[2]);

        TableStats boatsStats = boats.getStats();

        assertEquals(10_000, boatsStats.count);
        assertEquals(1, boatsStats.minimums[0]);
        assertEquals(4, boatsStats.minimums[1]);
        assertEquals(0, boatsStats.minimums[2]);
        assertEquals(10_000, boatsStats.maximums[0]);
        assertEquals(9_999, boatsStats.maximums[1]);
        assertEquals(10_000, boatsStats.maximums[2]);
    }
}
