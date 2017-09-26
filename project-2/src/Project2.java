import datastore.Database;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileReader;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Startup class for Project2
 * Loads the database and the queries file, runs all queries
 */
public class Project2 {
	private static String INPUT_PATH = "resources/samples/input";
	private static String DB_PATH = INPUT_PATH + "/db";

	public static void main(String args[]) {
		// TODO: read paths from command line

		Optional<Database> dbOpt = Database.loadDatabase(Paths.get(DB_PATH));

		if (!dbOpt.isPresent()) {
			System.out.println("Error loading database from " + Paths.get(DB_PATH));
			return;
		}

		Database DB = dbOpt.get();
		QueryBuilder builder = new QueryBuilder(DB);

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(INPUT_PATH + "/queries.sql"));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				System.out.println("Read statement: " + statement);
				// Get select body from statement
				PlainSelect select = (PlainSelect) ((Select) statement).getSelectBody();

				// Run query and print output
				builder.buildQuery(select).dump(System.out);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
