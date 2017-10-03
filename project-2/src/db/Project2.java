package db;

import db.datastore.Database;
import db.operators.Operator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import db.query.QueryBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Startup class for db.Project2
 * Loads the database and the queries file, runs all queries
 */
public class Project2 {
	public static String INPUT_PATH = "resources/samples/input";
	public static String DB_PATH = INPUT_PATH + "/db";

	public static String OUTPUT_PATH = "resources/samples/output";

	public static void main(String args[]) {
		if (args.length >= 2) {
			INPUT_PATH = args[0];
			DB_PATH = INPUT_PATH + "/db";
			OUTPUT_PATH = args[1];
			System.out.println(INPUT_PATH + "\n" + OUTPUT_PATH);
		}

		Database DB = Database.loadDatabase(Paths.get(DB_PATH));
		QueryBuilder builder = new QueryBuilder(DB);

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(INPUT_PATH + "/queries.sql"));
			Statement statement;
			int i = 1;

			// Create output directory if needed
			Files.createDirectories(Paths.get(OUTPUT_PATH));

			while ((statement = parser.Statement()) != null) {
				System.out.println("Read statement: " + statement);

				// Get select body from statement
				PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

				// Run db.query and print output
				Operator queryPlanRoot = builder.buildQuery(select);
				queryPlanRoot.dump(System.out, true);

				// Write output to file
				File outputFile = new File(OUTPUT_PATH + "/query" + i++);
				PrintStream stream = new PrintStream(new FileOutputStream(outputFile));

				queryPlanRoot.reset();
				queryPlanRoot.dump(stream, false);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
