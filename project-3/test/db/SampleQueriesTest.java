package db;

import db.PhysicalPlanConfig.JoinImplementation;
import db.PhysicalPlanConfig.SortImplementation;
import db.Utilities.UnionFind;
import db.Utilities.Utilities;
import db.datastore.Database;
import db.datastore.TableInfo;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.operators.physical.physical.ScanOperator;
import db.query.QueryBuilder;
import db.query.visitors.LogicalTreePrinter;
import db.query.visitors.PhysicalPlanBuilder;
import db.query.visitors.PhysicalTreePrinter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SampleQueriesTest {

    private LogicalOperator logicalOperator;
    private Operator queryPlanRoot;
    private ScanOperator sampleTuples;
    private boolean isOrdered;
    private String query;

    @Parameters(name = "join={3} sort={4} query={2} path={1}")
    public static Collection<Object[]> data() {
        ArrayList<Object[]> testCases = new ArrayList<>();

        Database DB = Database.loadDatabase(TestUtils.DB_PATH);

        try {
            CCJSqlParser parser = new CCJSqlParser(new FileReader(TestUtils.INPUT_PATH.resolve("queries.sql").toFile()));
            Statement statement;
            int i = 1;

            while ((statement = parser.Statement()) != null) {
                UnionFind unionFind = new UnionFind();
                QueryBuilder builder = new QueryBuilder(DB, unionFind);

                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
                LogicalOperator logicalPlan = builder.buildQuery(select);
                Path expectedFile = TestUtils.EXPECTED_PATH.resolve("query" + i++);

                for (JoinImplementation joinType : JoinImplementation.values()) {
                    for (SortImplementation sortType : SortImplementation.values()) {
                        if (joinType.equals(JoinImplementation.TNLJ)) {
                            continue;
                        }

                        testCases.add(new Object[]{logicalPlan, expectedFile, statement.toString(), joinType, sortType});
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return testCases;
    }

    public SampleQueriesTest(LogicalOperator logicalOperator, Path expectedFile, String query,
                             JoinImplementation joinType, SortImplementation sortType) {
        this.logicalOperator = logicalOperator;
        this.query = query;

        this.isOrdered = query.contains("ORDER BY") || query.contains("DISTINCT");

        PhysicalPlanConfig config = new PhysicalPlanConfig(joinType, sortType, 8, 16, false);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, TestUtils.TEMP_PATH, TestUtils.DB_PATH.resolve("indexes"));

        this.queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalOperator);

        TableInfo tableInfo = new TableInfo(queryPlanRoot.getHeader(), expectedFile, true);
        this.sampleTuples = new ScanOperator(tableInfo);
    }

    @After
    public void tearDown() throws Exception {
        if (this.queryPlanRoot != null) {
            this.queryPlanRoot.close();
        }

        this.sampleTuples.close();

        Utilities.cleanDirectory(TestUtils.TEMP_PATH);
    }

    @Test
    public void printDebugInfo() throws Exception {
        System.out.println("Query: " + this.query);
        System.out.println("Logical Tree:");
        LogicalTreePrinter.printTree(this.logicalOperator);
        System.out.println("Physical Tree:");
        PhysicalTreePrinter.printTree(this.queryPlanRoot);
    }
    
    @Test
    public void test() {
        if (isOrdered) {
            TestUtils.compareTuples(this.sampleTuples, this.queryPlanRoot);
        } else {
            TestUtils.unorderedCompareTuples(this.sampleTuples, this.queryPlanRoot);
        }
    }
}