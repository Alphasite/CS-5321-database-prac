package db;

import db.PhysicalPlanConfig.JoinImplementation;
import db.PhysicalPlanConfig.SortImplementation;
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

import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SampleQueriesTest {

    private static final String INPUT_PATH = "resources/samples/input";
    private static final String EXPECTED_PATH = "resources/samples/expected";
    private static final String TEMP_PATH = "resources/samples/tmp";

    private LogicalOperator logicalOperator;
    private Operator queryPlanRoot;
    private ScanOperator sampleTuples;
    private boolean isOrdered;
    private String query;

    @Parameters(name = "join={3} sort={4} query={2} path={1}")
    public static Collection<Object[]> data() {
        ArrayList<Object[]> testCases = new ArrayList<>();

        Database DB = Database.loadDatabase(Paths.get(Project3.DB_PATH));
        QueryBuilder builder = new QueryBuilder(DB);

        try {
            CCJSqlParser parser = new CCJSqlParser(new FileReader(INPUT_PATH + File.separator + "queries.sql"));
            Statement statement;
            int i = 1;

            while ((statement = parser.Statement()) != null) {
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
                LogicalOperator logicalPlan = builder.buildQuery(select);
                Path expectedFile = Paths.get(EXPECTED_PATH).resolve("query" + i++);

                for (JoinImplementation joinType : JoinImplementation.values()) {
                    for (SortImplementation sortType : SortImplementation.values()) {
                        if (joinType != JoinImplementation.SMJ) {
                            testCases.add(new Object[]{logicalPlan, expectedFile, statement.toString(), joinType, sortType});
                        }
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

        if (query.contains("ORDER BY") || query.contains("DISTINCT")) {
            this.isOrdered = true;
        } else {
            this.isOrdered = false;
        }

        PhysicalPlanConfig config = new PhysicalPlanConfig(joinType, sortType, 8, 16);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, Paths.get(TEMP_PATH));

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

        Utilities.cleanDirectory(Paths.get(Project3.TEMP_PATH));
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