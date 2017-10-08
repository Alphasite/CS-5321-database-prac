package db;

import db.datastore.Database;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.PhysicalPlanBuilder;
import db.query.QueryBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Scanner;

public class InteractiveShell {
    public static String INPUT_PATH = "resources/samples/input";
    public static String DB_PATH = INPUT_PATH + "/db";

    public static String OUTPUT_PATH = "resources/samples/output";

    /**
     * @param args If present : [inputFolder] [outputFolder]
     */
    public static void main(String args[]) {
        if (args.length >= 2) {
            INPUT_PATH = args[0];
            DB_PATH = INPUT_PATH + "/db";
            OUTPUT_PATH = args[1];
            System.out.println(INPUT_PATH + "\n" + OUTPUT_PATH);
        }

        Database DB = Database.loadDatabase(Paths.get(DB_PATH));
        QueryBuilder builder = new QueryBuilder(DB);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder();

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine();
            Reader stringReader = new StringReader(input);

            CCJSqlParser parser = new CCJSqlParser(stringReader);

            try {
                Statement statement = parser.Statement();

                // Get select body from statement
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

                // Run db.query and print output
                LogicalOperator logicalPlan = builder.buildQuery(select);

                // Create physical plan optimized for query on given data
                Operator queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalPlan);

                queryPlanRoot.dump(System.out, true);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
