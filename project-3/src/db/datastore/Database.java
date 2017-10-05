package db.datastore;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.*;

/**
 * This class acts as a catalogue which contains the database schema and references to the files which
 * contain the database tuples.
 */
public class Database {
    private final Map<String, TableInfo> tables;


    /**
     * Instantiate the catalogue.
     *
     * @param tables The mapping of table names to table definitions.
     */
    private Database(Map<String, TableInfo> tables) {
        this.tables = tables;
    }


    /**
     * A convenience method which loads the catalogue file from disk and creates the database object.
     *
     * @param inputPath the path of the folder which contains the database information.
     * @return A database catalogue instance.
     */
    public static Database loadDatabase(Path inputPath) {
        HashMap<String, TableInfo> tables = new HashMap<>();

        Path schema = inputPath.resolve("schema.txt");
        Path data = inputPath.resolve("data");

        try (Scanner s = new Scanner(new BufferedInputStream(new FileInputStream(schema.toFile())))) {
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] words = line.split(" ");

                String tableName = words[0];
                List<String> alias = new ArrayList<>();
                List<String> columns = new ArrayList<>();

                for (int i = 1; i < words.length; i++) {
                    alias.add(tableName);
                    columns.add(words[i]);
                }

                TableHeader header = new TableHeader(alias, columns);
                TableInfo tableInfo = new TableInfo(header, data.resolve(tableName));
                tables.put(tableName, tableInfo);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return (new Database(tables));
    }


    /**
     * Get information about a table by name.
     *
     * @param name The table name
     * @return The table info object.
     */
    public TableInfo getTable(String name) {
        return (this.tables.get(name));
    }
}
