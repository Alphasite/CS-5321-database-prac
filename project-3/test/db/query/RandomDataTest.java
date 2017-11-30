package db.query;

import db.PhysicalPlanConfig;
import db.PhysicalPlanConfig.JoinImplementation;
import db.TestUtils;
import db.datastore.Database;
import db.datastore.tuple.Tuple;
import db.operators.DummyOperator;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.visitors.LogicalTreePrinter;
import db.query.visitors.PhysicalTreePrinter;
import org.apache.commons.io.FileUtils;
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
            "SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND 150 < Sailors.B;",
            "SELECT DISTINCT * FROM Sailors;",
            "SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;",
            "SELECT B.F, B.D FROM Boats B ORDER BY B.D, B.F;",
            "SELECT DISTINCT B.F, B.D FROM Boats B ORDER BY B.D, B.F;",
            "SELECT DISTINCT B.F, B.D FROM Boats B;",
            "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C, S.A, S.B, R.G, R.H, B.D, B.E, B.F;",
            "SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C, S.A, S.B, R.G, R.H, B.D, B.E, B.F;",
            "SELECT * FROM Sailors S, Reserves R WHERE S.A = R.G AND R.H = S.B;",
            "SELECT * FROM Sailors S, Reserves R, Large L WHERE S.A = R.G AND R.H = L.I",
            "SELECT * FROM Large L1, Large L2 WHERE L1.I = L2.I",
            "SELECT * FROM Sailors WHERE Sailors.B = 100;", // used unclustered index
            "SELECT * FROM Sailors WHERE Sailors.B < 100;", // unused unclustered index
            "SELECT * FROM Sailors WHERE Sailors.A < 100;", // clustered index
            "SELECT * FROM Boats WHERE Boats.E = 500;", // used unclustered index
            "SELECT * FROM Boats WHERE Boats.E > 500;", // unused unclustered index
            "SELECT * FROM Sailors, Boats WHERE Sailors.A < 100 AND Boats.F > 900 AND Boats.F < 1000;", // both clustered indexes
            "SELECT * FROM Sailors, Boats WHERE Sailors.A < 100 AND Boats.E > 900 AND Boats.E < 1000;", // one clustered one unclustered
            "SELECT * FROM Sailors, Boats WHERE Sailors.B < 100 AND Boats.F > 900 AND Boats.F < 1000;", // one unclustered one clustered
            "SELECT * FROM Sailors, Boats WHERE Sailors.B < 100 AND Boats.E > 900 AND Boats.E < 1000;", // both unclustered
            "SELECT * FROM Sailors, Boats WHERE Sailors.C < 100 AND Boats.D > 900 AND Boats.D < 1000;", // both no indices
    };

    private static final int[] blockSizes = new int[]{1, 3, 11, 100};
    private final String query;
    private final LogicalOperator logical;

    private Operator physical;
    private Operator expectedResult;
    private boolean isOrdered;

    @Parameterized.Parameters(name = "{index}: join={3} sort={4} block={5} indices={6} query={2}")
    public static Collection<Object[]> data() throws IOException {
        List<Object[]> testCases = new ArrayList<>();

        Path dir = Files.createTempDirectory("RandomTest");

        Map<String, List<Tuple>> results = TestUtils.populateDatabase(dir, Arrays.asList(testQueries), ROWS_PER_TABLE, RAND_RANGE);

        for (String query : testQueries) {
            for (Boolean useIndices : Arrays.asList(true, false)) {
                for (int blockSize : blockSizes) {
                    // Neither sorting nor joining can handle tiny buffers.
                    if (blockSize >= 3) {
                        testCases.add(new Object[]{
                                new PhysicalPlanConfig(null, SortImplementation.EXTERNAL, blockSize, blockSize, useIndices),
                                results.get(query),
                                query,
                                null,
                                SortImplementation.EXTERNAL,
                                blockSize,
                                useIndices,
                                dir
                        });
                    }

                    for (JoinImplementation joinType : JoinImplementation.values()) {
                        for (SortImplementation sortType : SortImplementation.values()) {
                            if (joinType.equals(JoinImplementation.TNLJ)) {
                                continue;
                            }

                            if (blockSize == 1 && (sortType.equals(SortImplementation.EXTERNAL) || joinType.equals(JoinImplementation.SMJ))) {
                                continue;
                            }

                            testCases.add(new Object[]{new PhysicalPlanConfig(joinType, sortType, blockSize, blockSize, useIndices), results.get(query), query, joinType, sortType, blockSize, useIndices, dir});
                        }
                    }
                }
            }
        }

        return testCases;
    }

    public RandomDataTest(PhysicalPlanConfig config, List<Tuple> expected, String query, JoinImplementation join, SortImplementation sort, int blockSize, boolean useIndices, Path tempDir) throws IOException {
        Path path = Files.createTempDirectory("db-tempdir");
        FileUtils.copyDirectory(path.toFile(), tempDir.toFile());

        Database DB = Database.loadDatabase(tempDir);
        DB.buildIndexes();
        QueryBuilder builder = new QueryBuilder(DB);

        this.logical = builder.buildQuery(TestUtils.parseQuery(query));
        this.physical = TestUtils.getQueryPlan(tempDir, query, config);

        this.expectedResult = new DummyOperator(expected, physical.getHeader());
        this.query = query;
        this.isOrdered = query.contains("ORDER BY");
    }

    @After
    public void tearDown() throws Exception {
        this.physical.close();
        this.expectedResult.close();
    }

    @Test
    public void test() {
        System.out.println("Query: " + this.query);
        System.out.println("Logical Tree:");
        LogicalTreePrinter.printTree(this.logical);
        System.out.println("Physical Tree:");
        PhysicalTreePrinter.printTree(this.physical);

        if (isOrdered) {
            TestUtils.compareTuples(this.expectedResult, this.physical);
        } else {
            TestUtils.unorderedCompareTuples(this.expectedResult, this.physical);
        }
    }
}
