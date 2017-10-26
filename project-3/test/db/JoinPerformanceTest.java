package db;

import db.PhysicalPlanConfig.JoinImplementation;
import db.datastore.tuple.Tuple;
import db.operators.physical.Operator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@RunWith(Parameterized.class)
public class JoinPerformanceTest {
    private final static int ROWS_PER_TABLE = 5000;
    private final static int RAND_RANGE = 7500;

    private static String[] testQueries = new String[]{
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Sailors.A = Boats.D;",
            "SELECT * FROM Sailors S, Reserves R, WHERE S.A = R.G AND R.H = S.B;",
            "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D;"
    };

    private static final PhysicalPlanConfig[] testConfigs = new PhysicalPlanConfig[] {
            new PhysicalPlanConfig(JoinImplementation.TNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 1, 5),
            new PhysicalPlanConfig(JoinImplementation.BNLJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5),
            new PhysicalPlanConfig(JoinImplementation.SMJ, PhysicalPlanConfig.SortImplementation.EXTERNAL, 5, 5)
    };

    private Operator actualResult;
    private JoinImplementation join;
    private int blockSize;
    private int queryIdx;

    @Parameterized.Parameters(name = "{index}: join={3} block={4} query={1}")
    public static Collection<Object[]> data() throws IOException {
        ArrayList<Object[]> testCases = new ArrayList<>();

        Path dir = Files.createTempDirectory("RandomTest");

        Map<String, List<Tuple>> results = TestUtils.populateDatabase(dir, Arrays.asList(testQueries), ROWS_PER_TABLE, RAND_RANGE);

        int idx = 0;
        for (String query : testQueries) {
            for (PhysicalPlanConfig config : testConfigs) {
                testCases.add(new Object[] { config, query, idx, config.joinImplementation, config.joinParameter, dir });
            }
        }

        return testCases;
    }

    public JoinPerformanceTest(PhysicalPlanConfig config, String query, int queryIdx, JoinImplementation join, int blockSize, Path tempDir) {
        this.actualResult = TestUtils.getQueryPlan(tempDir, query, config);
        this.join = join;
        this.blockSize = blockSize;
        this.queryIdx = queryIdx;
    }

    @After
    public void tearDown() throws Exception {
        this.actualResult.close();
    }

    @Test
    public void test() {
        long startTime = System.currentTimeMillis();

        int outputRows = 0;
        while (this.actualResult.hasNextTuple()) {
            this.actualResult.getNextTuple();
            outputRows++;
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(this.join + " (blockSize=" + this.blockSize + ") " + outputRows + " rows found, took " + elapsedTime + " ms");
    }
}
