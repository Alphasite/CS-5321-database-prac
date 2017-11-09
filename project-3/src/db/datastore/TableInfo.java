package db.datastore;

import java.nio.file.Path;

/**
 * Encapsulates information about a table:
 * path on disk and the table header information (its schema effectively)
 */
public class TableInfo {
    public final String tableName;
    public final Path file;
    public final TableHeader header;
    public final boolean binary;

    /**
     * Create a new table info reference.
     *
     * @param header the header for tuples in this relation.
     * @param file   The file path for the relation
     * @param binary Whether or not the table is stored in binary form.
     */
    public TableInfo(TableHeader header, Path file, Boolean binary) {
        this.header = header;
        this.file = file;
        this.binary = binary;

        // Resolve table name from file name
        this.tableName = file.getFileName().toString();
    }
}
