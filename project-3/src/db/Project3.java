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
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Startup class for db.Project3
 * Loads the database and the queries file, runs all queries and outputs
 * results in specified output folder
 */
public class Project3 {

    public static boolean DUMP_TO_CONSOLE = false;
    private static final boolean CLEANUP = true;
    private static final boolean BINARY_OUTPUT = true;

    /**
     * @param args If present : [configFile]
     */
    public static void main(String args[]) {
        Path filePath = args.length >= 1 ? Paths.get(args[0]) : Paths.get("resources/samples/input/config.txt");
        GeneralConfig config = GeneralConfig.fromFile(filePath);

        Database DB = Database.loadDatabase(config.dbPath);

        if (config.buildIndexes) {
            DB.buildIndexes();
        }

        if (config.evaluateQueries) {
            try {
                runQueries(new FileReader(config.inputDir.resolve("queries.sql").toFile()), DB, config);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static void runQueries(Reader queryReader, Database DB, GeneralConfig config) {
        CCJSqlParser parser = new CCJSqlParser(queryReader);
        Statement statement;
        int i = 1;

        // Load plan config
        PhysicalPlanConfig planConfig = PhysicalPlanConfig.fromFile(config.inputDir.resolve("plan_builder_config.txt"));

        // Create directories if needed
        try {
            Files.createDirectories(config.outputDir);
            Files.createDirectories(config.tempDir);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            while ((statement = parser.Statement()) != null) {
                try {
                    System.out.println("Read statement: " + statement);

                    // Get select body from statement
                    PlainSelect selectQuery = (PlainSelect) ((Select) statement).getSelectBody();
                    Path outputFile = config.outputDir.resolve("query" + i++);

                    long start = System.currentTimeMillis();

                    runQuery(selectQuery, outputFile, config.tempDir, DB, planConfig);

                    System.out.println("Query executed in " + (System.currentTimeMillis() - start) + "ms");

                    if (CLEANUP) {
                        Utilities.cleanDirectory(config.tempDir);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing query: " + statement);
                    System.err.println("Trying next query...");
                    e.printStackTrace();
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static void runQuery(PlainSelect selectQuery, Path outputFile, Path tempDir, Database DB, PhysicalPlanConfig config) {
        QueryBuilder builder = new QueryBuilder(DB);
        PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config, tempDir);

        // Build logical query plan
        LogicalOperator logicalPlan = builder.buildQuery(selectQuery);

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

            if (BINARY_OUTPUT) {
                fileWriter = BinaryTupleWriter.get(queryPlanRoot.getHeader(), outputFile);
            } else {
                fileWriter = StringTupleWriter.get(outputFile);
            }

            queryPlanRoot.dump(fileWriter);

        } finally {
            queryPlanRoot.close();

            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

}
