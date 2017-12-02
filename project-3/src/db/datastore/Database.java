package db.datastore;

import db.datastore.index.BulkLoader;
import db.datastore.stats.StatsGatherer;
import db.datastore.stats.TableStats;

import java.io.IOException;
import java.nio.file.Files;
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
    private List<IndexInfo> indexes;

    /**
     * Instantiate the catalogue.
     *
     * @param tables The mapping of table names to table definitions.
     */
    private Database(Path dbPath, Map<String, TableInfo> tables) {
        this.dbPath = dbPath;

        this.tables = tables;
        this.indexes = new ArrayList<>();
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

        try (Scanner s = new Scanner(schema)) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }

        Database database = new Database(inputPath, tables);
        database.loadIndexInfo(inputPath.resolve("index_info.txt"));

        return database;
    }

    /**
     * If present, read index catalog info from file. Does not deserialize indexes.
     */
    private void loadIndexInfo(Path indexConfigFile) {
        try (Scanner scanner = new Scanner(indexConfigFile)) {
            while (scanner.hasNextLine()) {
                IndexInfo info = IndexInfo.parse(scanner.nextLine());

                indexes.add(info);

                // reference index in table schema
                tables.get(info.tableName).indices.add(info);
            }
        } catch (IOException e) {
            System.out.println("WARNING: No index_info.txt found, proceeding with 0 indices...");
        }
    }

    /**
     * Replaces the current set of indices with the given list of IndexInfo instances.
     * Mostly just for testing purposes.
     *
     * @param newIndexes The new set of indices
     */
    public void updateIndexes(List<IndexInfo> newIndexes) {
        indexes = newIndexes;

        for (TableInfo t : tables.values()) {
            t.indices = new ArrayList<>();
        }

        for (IndexInfo info : indexes) {
            tables.get(info.tableName).indices.add(info);
        }
    }

    /**
     * Build the indices as guided by the config file.
     */
    public void buildIndexes() {
        Path indexFolder = dbPath.resolve("indexes");

        try {
            if (!Files.exists(indexFolder)) {
                Files.createDirectory(indexFolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (IndexInfo config : indexes) {
            BulkLoader.buildIndex(this, config, indexFolder);
        }
    }

    /**
     * Compute statistics for every base table and save them inside main folder.
     */
    public void writeStatistics() {
        List<TableStats> stats = StatsGatherer.gatherStats(this);
        StatsGatherer.writeStatsFile(this.dbPath, StatsGatherer.asString(stats));
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

    public List<TableInfo> getTables() {
        return new ArrayList<>(tables.values());
    }
}
