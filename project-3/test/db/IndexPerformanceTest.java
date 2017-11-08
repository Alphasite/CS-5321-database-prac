package db;

import db.PhysicalPlanConfig.JoinImplementation;
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

@RunWith(Parameterized.class)
public class IndexPerformanceTest {
    private static final Path indexFile = Paths.get("resources/samples-4/expected_indexes/Boats.E").toAbsolutePath();
    private static final Path inputDir = Paths.get("resources/samples-4/input/db").toAbsolutePath();

    private static String[] testQueries = new String[]{
            // Both indexed
            "SELECT * FROM Sailors S, Boats B WHERE Boats.E = Sailors.A AND Boats.E > 100 AND Boats.E < 1000 AND Sailors.A > 100 AND Sailors.A < 1000",
            // 1 indexed
            "SELECT * FROM Sailors S, Boats B WHERE Boats.E = Sailors.A AND Boats.E > 100 AND Boats.E < 1000",
            // 1 indexed specific
            "SELECT * FROM Sailors S, Boats B WHERE Boats.E = Sailors.A AND Boats.E = 100",
            // 1 indexed specific & 1 indexed general
            "SELECT * FROM Sailors S, Boats B WHERE Boats.E = Sailors.A AND Boats.E = 100 AND Sailors.A > 100 AND Sailors.A < 1000",
            // No indexed
            "SELECT * FROM Sailors S, Boats B WHERE Boats.E = Sailors.A",
    };

    private static final PhysicalPlanConfig[] testConfigs = new PhysicalPlanConfig[]{
            new PhysicalPlanConfig(JoinImplementation.TNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 1, 5, false),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 1, 5, false),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, false),
            new PhysicalPlanConfig(JoinImplementation.SMJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, false),
            new PhysicalPlanConfig(JoinImplementation.TNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 1, 5, true),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 1, 5, true),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, true),
            new PhysicalPlanConfig(JoinImplementation.SMJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5, true)
    };

    private Operator actualResult;
    private JoinImplementation join;
    private boolean isIndexed;

    private long elapsedTime;
    private int outputRows;

    @Parameterized.Parameters(name = "{index}: indexed={3} join={2} query={1}")
    public static Collection<Object[]> data() throws IOException {
        ArrayList<Object[]> testCases = new ArrayList<>();

        for (String query : testQueries) {
            for (PhysicalPlanConfig config : testConfigs) {
                testCases.add(new Object[]{config, query, config.joinImplementation, config.useIndices});
            }
        }

        return testCases;
    }

    public IndexPerformanceTest(PhysicalPlanConfig config, String query, JoinImplementation join, boolean isIndexed) {
        this.actualResult = TestUtils.getQueryPlan(inputDir, query, config);
        this.join = join;
        this.isIndexed = isIndexed;
    }

    @Before
    public void setUp() throws Exception {
        DiskIOStatistics.reads = 0;
        DiskIOStatistics.writes = 0;
    }

    @After
    public void tearDown() throws Exception {
        this.actualResult.close();

        System.out.println("indexed=" + this.isIndexed + " join=" + this.join.toString() + outputRows + " rows found, took " + elapsedTime + " ms");
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
    }
}
