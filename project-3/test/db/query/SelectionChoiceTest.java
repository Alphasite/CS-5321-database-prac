package db.query;

import db.PhysicalPlanConfig;
import db.TestUtils;
import db.datastore.Database;
import db.datastore.IndexInfo;
import db.datastore.index.BulkLoader;
import db.operators.UnaryNode;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.operators.physical.bag.ProjectionOperator;
import db.operators.physical.bag.SelectionOperator;
import db.operators.physical.physical.IndexScanOperator;
import db.operators.physical.physical.ScanOperator;
import db.query.visitors.PhysicalPlanBuilder;
import db.query.visitors.PhysicalTreePrinter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SelectionChoiceTest {
    private static Path path;
    private static Database database;

    private static final List<IndexInfo> indexConfig = Arrays.asList(
            new IndexInfo("Sailors", "A", true, 15),
            new IndexInfo("Boats", "E", true, 15),
            new IndexInfo("Sailors", "C", false, 15),
            new IndexInfo("Boats", "D", false, 15)
    );

    /* Calculations:

    Stats (copy-pasted):
        Boats 10000 D,1,10000 E,4,9999 F,0,10000
        Sailors 10000 A,1,10000 B,0,10000 C,2,9998

    Data:
        Number of pages in Boats/Sailors: 122,880 / 4096 = 30
        Number of tuples in Boats/Sailors: 10,000

        (experimentally gotten, not calculated)
        Number of leaves in Sailors.C = 211
        Number of leaves in Boats.D = 212

    Costs:
        Cost of full table scan (both Sailors and Boats): 10,000 * 12 / 4096 = 29.297

        Cost of Sailors.A index: 3 + 30 * r
        Cost of Boats.E index: 3 + 30 * r

        Cost of Sailors.C index: 3 + 211 * r + 10,000 * r
        Cost of Boats.D index: 3 + 212 * r + 10,000 * r

     */
    private static String[] testQueries = new String[] {
            "SELECT * FROM Sailors WHERE Sailors.A > 100 AND Sailors.A < 200", // r = 0.0099, cost of Sailors.A = 3.297
            "SELECT * FROM Sailors WHERE Sailors.C = 100",                     // r = 0.0001, cost of Sailors.C = 4.02
            "SELECT * FROM Sailors WHERE Sailors.A > 5000",                    // r = 0.5, cost of Sailors.A = 18
            "SELECT * FROM Sailors WHERE Sailors.C > 5000",                    // r = 0.4999, cost of Sailors.C = 5107
            "SELECT * FROM Boats WHERE Boats.E >= 5000 AND Boats.E < 6000",    // r = 0.1, cost of Boats.E = 6
            "SELECT * FROM Boats WHERE Boats.D >= 5000 AND Boats.D < 6000",    // r = 0.1, cost of Boats.D = 1024.2
            "SELECT * FROM Boats WHERE Boats.E >= 1000 AND Boats.E < 1025",    // r = 0.0025, cost of Boats.E = 3.075
            "SELECT * FROM Boats WHERE Boats.D >= 1000 AND Boats.D < 1025",    // r = 0.0025, cost of Boats.D = 28.53

            "SELECT * FROM Sailors WHERE Sailors.A > 5000 AND Sailors.C <= 50", // r = 0.5, cost of Sailors.A = 18
                                                                                // r = 0.0049, cost of Sailors.C = 53.0339

            "SELECT * FROM Boats WHERE Boats.E > 1000 AND Boats.D >= 1000 AND Boats.D < 1025", // r = 0.9003, cost of Boats.E = 30.009
                                                                                               // r = 0.0025, cost of Boats.D = 28.53

            "SELECT * FROM Sailors WHERE Sailors.A <= 9970 AND Sailors.C > 9970", // r = 0.997, cost of Sailors.A = 32.91
                                                                                  // r = 0.0028, cost of Sailors.C = 31.5908
    };

    private static String[] expectedIndices = new String[] {
            "Sailors.A",
            "Sailors.C",
            "Sailors.A",
            null,
            "Boats.E",
            null,
            "Boats.E",
            "Boats.D",
            "Sailors.A",
            "Boats.D",
            null
    };

    private Operator actualResult;
    private String expectedIndex;

    @Parameterized.Parameters(name = "query={0}, expectedIndex={1}")
    public static Collection<Object[]> data() {
        ArrayList<Object[]> testCases = new ArrayList<>();

        for (int i = 0; i < testQueries.length; i++) {
            testCases.add(new Object[]{testQueries[i], expectedIndices[i]});
        }

        return testCases;
    }

    public SelectionChoiceTest(String query, String expectedIndex) throws IOException {
        if (path == null || database == null) {
            path = Files.createTempDirectory("db-tempdir");
            FileUtils.copyDirectory(TestUtils.NEW_DB_PATH.toFile(), this.path.toFile());

            database = Database.loadDatabase(path);
            database.updateIndexes(indexConfig);
            database.buildIndexes();
        }

        QueryBuilder builder = new QueryBuilder(database);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(PhysicalPlanConfig.DEFAULT_CONFIG, SelectionChoiceTest.path, SelectionChoiceTest.path.resolve("indexes"));

        try {
            CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
            Statement statement = parser.Statement();

            PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
            LogicalOperator logicalPlan = builder.buildQuery(select);

            actualResult = physicalBuilder.buildFromLogicalTree(logicalPlan);

            this.expectedIndex = expectedIndex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.actualResult.close();
    }

    @Test
    public void test() {
        PhysicalTreePrinter.printTree(this.actualResult);

        while (actualResult instanceof UnaryNode) {
            actualResult = (Operator) ((UnaryNode) actualResult).getChild();
        }

        if (expectedIndex == null) {
            assertTrue(actualResult instanceof ScanOperator);
        } else {
            assertTrue(actualResult instanceof IndexScanOperator);

            IndexScanOperator indexScan = (IndexScanOperator) actualResult;
            assertEquals(expectedIndex, indexScan.getIndex().tableName + "." + indexScan.getIndex().attributeName);
        }
    }
}
