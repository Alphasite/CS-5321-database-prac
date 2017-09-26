package datastore;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.*;

public class Database {
    private final Map<String, TableInfo> tables;

    private Database(Map<String, TableInfo> tables) {
        this.tables = tables;
    }

    public static Optional<Database> loadDatabase(Path inputPath) {
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
            return Optional.empty();
        }

        return Optional.of(new Database(tables));
    }

    public Optional<TableInfo> getTable(String name) {
        return Optional.ofNullable(this.tables.get(name));
    }
}
