package db;

import db.PhysicalPlanConfig.JoinImplementation;
import db.datastore.Database;
import db.datastore.IndexInfo;
import db.datastore.index.BulkLoader;
import db.operators.physical.Operator;
import db.performance.DiskIOStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class IndexPerformanceTest {
    private static final Path inputDir = Paths.get("resources/samples-4/input/db").toAbsolutePath();
    private static Database database;

    private static String[] testQueries = new String[]{
            // Both indexed
            "SELECT * FROM Sailors, Boats WHERE Boats.E = Sailors.A AND Boats.E > 100 AND Boats.E < 1000 AND Sailors.A > 100 AND Sailors.A < 1000",
            // 1 indexed
            "SELECT * FROM Sailors, Boats WHERE Boats.E = Sailors.A AND Boats.E > 100 AND Boats.E < 1000",
            // 1 indexed specific
            "SELECT * FROM Sailors, Boats WHERE Boats.E = Sailors.A AND Boats.E = 100 AND Boats.F = Sailors.B",
            // 1 indexed specific & 1 indexed general
            "SELECT * FROM Sailors, Boats WHERE Boats.E = Sailors.A AND Boats.E = 100 AND Sailors.A > 100 AND Sailors.A < 1000",
            // No indexed
            "SELECT * FROM Sailors, Boats WHERE Boats.E = Sailors.A"
    };

    private static PhysicalPlanConfig testConfigIndex = new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, true);
    private static PhysicalPlanConfig testConfigNoIndex = new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, false);

    private Operator actualResult;
    private long elapsedTime;
    private int outputRows;

    @Parameterized.Parameters(name = "{index}: <{3}> query={0}")
    public static Collection<Object[]> data() throws IOException {
        ArrayList<Object[]> testCases = new ArrayList<>();

        List<IndexInfo> indexConfigEmpty = new ArrayList<>();

        List<IndexInfo> indexConfigUnclustered = new ArrayList<>();
        indexConfigUnclustered.add(new IndexInfo("Sailors", "A", false, 15));
        indexConfigUnclustered.add(new IndexInfo("Boats", "E", false, 15));

        List<IndexInfo> indexConfigClustered = new ArrayList<>();
        indexConfigClustered.add(new IndexInfo("Sailors", "A", true, 15));
        indexConfigClustered.add(new IndexInfo("Boats", "E", true, 15));

        for (String query : testQueries) {
            testCases.add(new Object[]{query, testConfigNoIndex, indexConfigEmpty, "No Index"});                    // no index
            testCases.add(new Object[]{query, testConfigIndex, indexConfigUnclustered, "Unclustered Index"});       // unclustered index
            testCases.add(new Object[]{query, testConfigIndex, indexConfigClustered, "Clustered Index"});           // clustered index
        }

        return testCases;
    }

    public IndexPerformanceTest(String query, PhysicalPlanConfig testConfig, List<IndexInfo> indexConfigs, String indexType) {
        for (IndexInfo indexConfig : indexConfigs) {
            BulkLoader.buildIndex(database, indexConfig, TestUtils.NEW_DB_PATH.resolve("indexes"));
        }
        this.actualResult = TestUtils.getQueryPlan(inputDir, query, testConfig);
        System.out.println("<" + indexType + "> query=" + query);
    }

    @Before
    public void setUp() throws Exception {
        System.out.println();
        database = Database.loadDatabase(inputDir);

        DiskIOStatistics.reads = 0;
        DiskIOStatistics.writes = 0;
    }

    @After
    public void tearDown() throws Exception {
        this.actualResult.close();

        System.out.println("Reads: " + DiskIOStatistics.reads);
        System.out.println("Write: " + DiskIOStatistics.writes);
    }

    @Test
    public void test() {
        long startTime = System.currentTimeMillis();

        while (this.actualResult.hasNextTuple()) {
            this.actualResult.getNextTuple();
            outputRows++;
        }

        elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println("Query took " + elapsedTime + " ms and found " + outputRows + " rows!");
    }
}
