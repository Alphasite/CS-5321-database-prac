package datastore;

import java.nio.file.Path;

public class TableInfo {
    public final Path file;
    public final TableHeader header;

    public TableInfo(TableHeader header, Path file) {
        this.header = header;
        this.file = file;
    }
}
