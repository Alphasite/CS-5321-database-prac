package db;

import db.datastore.Database;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.QueryBuilder;
import db.query.visitors.LogicalTreePrinter;
import db.query.visitors.PhysicalPlanBuilder;
import db.query.visitors.PhysicalTreePrinter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class InteractiveShell {
    private static String INPUT_PATH = "resources/samples/input";
    private static String DB_PATH = INPUT_PATH + "/db";
    private static String INDEXES_PATH = DB_PATH + "/indexes";

    private static String OUTPUT_PATH = "resources/samples/output";
    private static String TEMP_PATH = "resources/samples/tmp";

    private static final boolean WRITE_OUTPUT_TO_FILE = false;
    private static final boolean BINARY_OUTPUT = false;

    private static final boolean DUMP_TREE_INFO = true;

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

        Scanner scanner = new Scanner(System.in);

        int counter = 1;
        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine();

            if (input.equals("exit")) {
                return;
            }

            Reader stringReader = new StringReader(input);

            CCJSqlParser parser = new CCJSqlParser(stringReader);

            try {
                Statement statement = parser.Statement();

                // Get select body from statement
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

                // Build logical query plan
                QueryBuilder builder = new QueryBuilder(DB);
                LogicalOperator logicalPlan = builder.buildQuery(select);

                if (DUMP_TREE_INFO) {
                    LogicalTreePrinter.printTree(logicalPlan);
                }

                // Create physical plan optimized for query on given data
                PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(Paths.get(TEMP_PATH), Paths.get(INDEXES_PATH));
                Operator queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalPlan);

                if (DUMP_TREE_INFO) {
                    PhysicalTreePrinter.printTree(queryPlanRoot);
                }

                TupleWriter writer = new StringTupleWriter(System.out);
                queryPlanRoot.dump(writer);

                if (WRITE_OUTPUT_TO_FILE) {
                    Path outputFile = Paths.get(OUTPUT_PATH).resolve("interactive" + counter);
                    TupleWriter fileWriter;

                    if (BINARY_OUTPUT) {
                        fileWriter = BinaryTupleWriter.get(queryPlanRoot.getHeader(), outputFile);
                    } else {
                        fileWriter = StringTupleWriter.get(outputFile);
                    }

                    queryPlanRoot.reset();
                    queryPlanRoot.dump(fileWriter);

                    fileWriter.close();
                }

                queryPlanRoot.close();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            counter++;
        }
    }
}
