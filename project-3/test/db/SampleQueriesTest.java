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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class SampleQueriesTest {

    private static String INPUT_PATH = "resources/samples/input";
    private static String OUTPUT_PATH = "resources/samples/output";
    private static String EXPECTED_PATH = "resources/samples/expected";

    private LogicalOperator logicalOperator;
    private Operator queryPlanRoot;
    private ScanOperator sampleTuples;
    private boolean isOrdered;
    private String query;

    @Parameters(name = "{3} ({2})")
    public static Collection<Object[]> data() {
        ArrayList<Object[]> testCases = new ArrayList<>();

        Database DB = Database.loadDatabase(Paths.get(INPUT_PATH + File.separator + "db"));
        QueryBuilder builder = new QueryBuilder(DB);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder();


        try {
            CCJSqlParser parser = new CCJSqlParser(new FileReader(INPUT_PATH + File.separator + "queries.sql"));
            Statement statement;
            int i = 1;

            while ((statement = parser.Statement()) != null) {
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
                LogicalOperator logicalPlan = builder.buildQuery(select);
                Operator queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalPlan);

                File expectedFile = new File(EXPECTED_PATH + File.separator + "query" + i);
                testCases.add(new Object[]{logicalPlan, queryPlanRoot, expectedFile, statement.toString()});

                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return testCases;
    }

    public SampleQueriesTest(LogicalOperator logicalOperator, Operator queryPlanRoot, File expectedFile, String query) {
        this.logicalOperator = logicalOperator;
        this.queryPlanRoot = queryPlanRoot;
        this.query = query;

        if (query.contains("ORDER BY")) {
            this.isOrdered = true;
        } else {
            this.isOrdered = false;
        }

        TableInfo tableInfo = new TableInfo(queryPlanRoot.getHeader(), expectedFile.toPath(), true);
        this.sampleTuples = new ScanOperator(tableInfo);
    }

    @Test
    public void printQuery() throws Exception {
        System.out.println("Query: " + this.query);
    }

    @Test
    public void printLogicalTree() throws Exception {
        System.out.println("Logical Tree:");
        LogicalTreePrinter.printTree(this.logicalOperator);
    }

    @Test
    public void printPhysicalTree() throws Exception {
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