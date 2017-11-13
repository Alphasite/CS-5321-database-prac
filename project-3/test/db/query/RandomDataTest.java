package db.query;

import db.PhysicalPlanConfig;
import db.PhysicalPlanConfig.JoinImplementation;
import db.TestUtils;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.physical.Operator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static db.PhysicalPlanConfig.SortImplementation;

@RunWith(Parameterized.class)
public class RandomDataTest {
    private final static int ROWS_PER_TABLE = 1500;
    private final static int RAND_RANGE = 3000;

    private static String[] testQueries = new String[]{
            "SELECT * FROM Sailors;",
            "SELECT Sailors.A FROM Sailors;",
            "SELECT Boats.F, Boats.D FROM Boats;",
            "SELECT Reserves.G, Reserves.H FROM Reserves;",
            "SELECT * FROM Sailors WHERE Sailors.B >= Sailors.C;",
            "SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C;",
            "SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C;",
            "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;",
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;",
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B < 150;",
            "SELECT DISTINCT * FROM Sailors;",
            "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;",
            "SELECT B.F, B.D FROM Boats B ORDER BY B.D, B.F;",
            "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C, S.A, S.B, R.G, R.H, B.D, B.E, B.F;",
            "SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C, S.A, S.B, R.G, R.H, B.D, B.E, B.F;",
            "SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G AND R.H = S.B;",
    };

    private static final int[] blockSizes = new int[]{10, 50, 100};

    private Operator actualResult;
    private Operator expectedResult;
    private boolean isOrdered;

    @Parameterized.Parameters(name = "{index}: join={3} sort={4} block={5} query={2}")
    public static Collection<Object[]> data() throws IOException {
        ArrayList<Object[]> testCases = new ArrayList<>();

        Path dir = Files.createTempDirectory("RandomTest");

        Map<String, List<Tuple>> results = TestUtils.populateDatabase(dir, Arrays.asList(testQueries), ROWS_PER_TABLE, RAND_RANGE);

        for (String query : testQueries) {
            for (JoinImplementation joinType : JoinImplementation.values()) {
                for (SortImplementation sortType : SortImplementation.values()) {
                    for (int blockSize : blockSizes) {
                        testCases.add(new Object[]{new PhysicalPlanConfig(joinType, sortType, blockSize, blockSize, false), results.get(query), query, joinType, sortType, blockSize, dir});
                    }
                }
            }
        }

        return testCases;
    }

    public RandomDataTest(PhysicalPlanConfig config, List<Tuple> expected, String query, JoinImplementation join, SortImplementation sort, int blockSize, Path tempDir) {
        this.actualResult = TestUtils.getQueryPlan(tempDir, query, config);
        this.expectedResult = new DummyOperator(expected, actualResult.getHeader());
        this.isOrdered = query.contains("ORDER BY");
    }

    @After
    public void tearDown() throws Exception {
        this.actualResult.close();
        this.expectedResult.close();
    }

    @Test
    public void test() {
        if (isOrdered) {
            TestUtils.compareTuples(this.expectedResult, this.actualResult);
        } else {
            TestUtils.unorderedCompareTuples(this.expectedResult, this.actualResult);
        }
    }
}
