package db;

import db.datastore.Database;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.logical.LogicalOperator;
import db.operators.physical.Operator;
import db.query.QueryBuilder;
import db.query.visitors.PhysicalPlanBuilder;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Startup class for db.Project3
 * Loads the database and the queries file, runs all queries and outputs
 * results in specified output folder
 */
public class Project3 {
    public static String INPUT_PATH = "resources/samples/input";
    public static String DB_PATH = INPUT_PATH + "/db";

    public static String OUTPUT_PATH = "resources/samples/output";
    public static String TEMP_PATH = "resources/samples/tmp";

    public static boolean DUMP_TO_CONSOLE = false;
    private static final boolean CLEANUP = true;
    private static final boolean BINARY_OUTPUT = true;

    /**
     * @param args If present : [inputFolder] [outputFolder] [tempFolder]
     */
    public static void main(String args[]) {
        if (args.length >= 2) {
            INPUT_PATH = args[0];
            DB_PATH = INPUT_PATH + "/db";
            OUTPUT_PATH = args[1];
            TEMP_PATH = args[2];
            System.out.println(INPUT_PATH + "\n" + OUTPUT_PATH + "\n" + TEMP_PATH);
        }

        Database DB = Database.loadDatabase(Paths.get(DB_PATH));
        QueryBuilder builder = new QueryBuilder(DB);

        try {
            CCJSqlParser parser = new CCJSqlParser(new FileReader(INPUT_PATH + "/queries.sql"));
            Statement statement;
            int i = 1;

            // Load plan config
            PhysicalPlanConfig config = PhysicalPlanConfig.fromFile(Paths.get(INPUT_PATH + "/plan_builder_config.txt"));

            // Create directories if needed
            Files.createDirectories(Paths.get(OUTPUT_PATH));
            Files.createDirectories(Paths.get(TEMP_PATH));

            PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, Paths.get(TEMP_PATH));

            while ((statement = parser.Statement()) != null) {
                System.out.println("Read statement: " + statement);

                // Get select body from statement
                PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

                // Build logical query plan
                LogicalOperator logicalPlan = builder.buildQuery(select);

                // Create physical plan optimized for query on given data
                Operator queryPlanRoot = physicalBuilder.buildFromLogicalTree(logicalPlan);

                // Write output to file
                TupleWriter fileWriter = null;

                try {
                    if (DUMP_TO_CONSOLE) {
                        TupleWriter consoleWriter = new StringTupleWriter(System.out);
                        queryPlanRoot.dump(consoleWriter);
                        queryPlanRoot.reset();
                    }

                    Path outputFile = Paths.get(OUTPUT_PATH + "/query" + i++);
                    if (BINARY_OUTPUT) {
                        fileWriter = BinaryTupleWriter.get(queryPlanRoot.getHeader(), outputFile);
                    } else {
                        fileWriter = StringTupleWriter.get(outputFile);
                    }

                    long start = System.currentTimeMillis();

                    queryPlanRoot.dump(fileWriter);

                    System.out.println("Query executed in " + (System.currentTimeMillis() - start) + "ms");

                    if (CLEANUP) {
                        Utilities.cleanDirectory(Paths.get(TEMP_PATH));
                    }
                } finally {
                    queryPlanRoot.close();

                    if (fileWriter != null) {
                        fileWriter.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
