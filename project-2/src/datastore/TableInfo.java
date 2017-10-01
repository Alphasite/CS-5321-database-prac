package datastore;

import java.nio.file.Path;

/**
 * An class which encapsulates the information about the table.
 * Its path on disk and the table header information (its schema effectively)
 */
public class TableInfo {
    public final Path file;
    public final TableHeader header;

    public TableInfo(TableHeader header, Path file) {
        this.header = header;
        this.file = file;
    }
}
