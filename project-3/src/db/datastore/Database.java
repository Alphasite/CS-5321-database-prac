package db.datastore;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * This class acts as a catalogue which contains the database schema and references to the files which
 * contain the database tuples.
 */
public class Database {
    public static final int PAGE_SIZE = 4096;

    private final Path dbPath;

    private final Map<String, TableInfo> tables;
    private List<IndexInfo> indexInfo;

    /**
     * Instantiate the catalogue.
     *
     * @param tables The mapping of table names to table definitions.
     */
    private Database(Path dbPath, Map<String, TableInfo> tables) {
        this.dbPath = dbPath;

        this.tables = tables;
        this.indexInfo = new ArrayList<>();
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
                TableInfo tableInfo = new TableInfo(header, data.resolve(tableName), true);
                tables.put(tableName, tableInfo);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Database database = new Database(inputPath, tables);
        database.loadIndexInfo(inputPath.resolve("index_info.txt"));

        return database;
    }

    private void loadIndexInfo(Path indexConfigFile) {
        try {
            Scanner scanner = new Scanner(indexConfigFile);

            while (scanner.hasNextLine()) {
                IndexInfo info = IndexInfo.parse(scanner.nextLine());
                indexInfo.add(info);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void buildIndexes() {
        // Directpry is guaranteed to exist, no need to create it
        Path indexFolder = dbPath.resolve("indexes");


    }

    /**
     * Get information about a table by name.
     *
     * @param name The table name
     * @return The table info object.
     */
    public TableInfo getTable(String name) {
        return this.tables.get(name);
    }

}
