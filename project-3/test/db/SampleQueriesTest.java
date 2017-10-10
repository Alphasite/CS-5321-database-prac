package db;

import db.datastore.Database;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.PhysicalPlanBuilder;
import db.query.QueryBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SampleQueriesTest {

    private static String INPUT_PATH = "resources/samples/input";
    private static String OUTPUT_PATH = "resources/samples/output";
    private static String EXPECTED_PATH = "resources/samples/expected";

    @Parameters(name = "{1}: {2}")
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
                testCases.add(new Object[]{queryPlanRoot, expectedFile, statement.toString()});

                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return testCases;
    }

    private Operator queryPlanRoot;
    private BinaryTupleReader reader;

    public SampleQueriesTest(Operator queryPlanRoot, File expectedFile, String query) {
        this.queryPlanRoot = queryPlanRoot;

        try {
            this.reader = new BinaryTupleReader(null, new FileInputStream(expectedFile).getChannel());
        } catch (FileNotFoundException e) {
            fail("file '" + expectedFile.getName() + "' not found in expected folder");
        }
    }

    @Test
    public void test() {
        while (true) {
            Tuple expectedTuple = reader.next();
            Tuple outputTuple = queryPlanRoot.getNextTuple();

            if (outputTuple == null && expectedTuple == null) {
                break;
            } else if (outputTuple == null) {
                fail("output has fewer tuples than expected");
            } else if (expectedTuple == null) {
                fail("output has more tuples than expected");
            } else {
                assertEquals(expectedTuple, outputTuple);
            }
        }
    }

}