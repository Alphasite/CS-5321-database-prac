package db;

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

    @Parameters(name = "join={4} sort={5} query={3} path={2}")
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
                Path expectedFile = Paths.get(EXPECTED_PATH).resolve("query" + i);

                for (PhysicalPlanConfig.JoinImplementation joinType : PhysicalPlanConfig.JoinImplementation.values()) {
                    for (PhysicalPlanConfig.SortImplementation sortType : PhysicalPlanConfig.SortImplementation.values()) {
                        PhysicalPlanConfig config = new PhysicalPlanConfig(joinType, sortType);
                        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, Paths.get(TEMP_PATH));
                        Operator queryPlanRootTuple = physicalBuilder.buildFromLogicalTree(logicalPlan);
                        testCases.add(new Object[]{logicalPlan, queryPlanRootTuple, expectedFile, statement.toString(), joinType, sortType});
                    }
                }

                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return testCases;
    }

    public SampleQueriesTest(LogicalOperator logicalOperator, Operator queryPlanRoot, Path expectedFile, String query, String joinType, String sortType) {
        this.logicalOperator = logicalOperator;
        this.queryPlanRoot = queryPlanRoot;
        this.query = query;

        if (query.contains("ORDER BY")) {
            this.isOrdered = true;
        } else {
            this.isOrdered = false;
        }

        TableInfo tableInfo = new TableInfo(queryPlanRoot.getHeader(), expectedFile, true);
        this.sampleTuples = new ScanOperator(tableInfo);
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