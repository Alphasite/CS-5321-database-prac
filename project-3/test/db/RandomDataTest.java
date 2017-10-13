package db;

import db.datastore.Database;
import db.datastore.TableInfo;
import db.datastore.tuple.TupleReader;
import db.datastore.tuple.string.StringTupleReader;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.operators.physical.physical.ScanOperator;
import db.query.QueryBuilder;
import db.query.visitors.PhysicalPlanBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class RandomDataTest {

    private final static String DB_PATH = "resources/samples/testData";
    private final static String MYSQL_DB_NAME = "testdb";

    private static String[] testQueries = new String[] {
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
            "SELECT B.F, B.D FROM Boats B ORDER BY B.D;",
            "SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;",
            "SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;"
    };

    private Operator actualResult;
    private Operator expectedResult;
    private boolean isOrdered;

    @Parameterized.Parameters(name = "{2}")
    public static Collection<Object[]> data() throws IOException {
        ArrayList<Object[]> testCases = new ArrayList<>();

        // generate data and insert into MySQL
        TestUtils.generateRandomData(DB_PATH, 100, 100);
        try {
            TestUtils.executeBashCmd(String.format("mysql -u root testdb < %s/data/Boats_sql.sql", DB_PATH));
            TestUtils.executeBashCmd(String.format("mysql -u root testdb < %s/data/Sailors_sql.sql", DB_PATH));
            TestUtils.executeBashCmd(String.format("mysql -u root testdb < %s/data/Reserves_sql.sql", DB_PATH));
        } catch (InterruptedException e) {
            fail("unable to insert new data into MySQL");
        }

        Database DB = Database.loadDatabase(Paths.get(DB_PATH));
        QueryBuilder builder = new QueryBuilder(DB);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder();

        try {
            CCJSqlParser parser = new CCJSqlParser(new StringReader(String.join("", testQueries)));
            Statement statement;

            while ((statement = parser.Statement()) != null) {
                // get result from PhysicalPlanBuilder
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();
                LogicalOperator logicalPlan = builder.buildQuery(select);
                Operator queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalPlan);

                // get result from MySQL
                String command = String.format("echo '%s' | mysql -N -u root %s", statement.toString(), MYSQL_DB_NAME);
                Scanner expectedScanner = new Scanner(TestUtils.executeBashCmd(command)).useDelimiter("\\s+");
                TupleReader expectedTuples = new StringTupleReader(new TableInfo(queryPlanRoot.getHeader(), null, false), expectedScanner);
                ScanOperator expectedResult = new ScanOperator(expectedTuples);

                testCases.add(new Object[]{queryPlanRoot, expectedResult, statement.toString()});
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return testCases;
    }

    public RandomDataTest(Operator logicalOperator, Operator expectedResult, String query) {
        this.actualResult = logicalOperator;
        this.expectedResult = expectedResult;
        this.isOrdered = query.contains("ORDER BY");
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
