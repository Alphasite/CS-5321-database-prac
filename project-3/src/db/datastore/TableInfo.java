package db.datastore;

import db.datastore.stats.StatsGatherer;
import db.datastore.stats.TableStats;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates information about a table:
 * path on disk and the table header information (its schema effectively)
 */
public class TableInfo {
    public final String tableName;
    public final Path file;
    public final TableHeader header;
    public final boolean binary;
    public List<IndexInfo> indices;
    private TableStats stats;

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

        this.indices = new ArrayList<>();
    }

    /**
     * Get the stats for this table.
     *
     * @return the table's stats.
     */
    public TableStats getStats() {
        if (this.stats == null) {
            this.stats = StatsGatherer.gatherStats(this);
        }

        return this.stats;
    }
}
