package datastore;

import java.nio.file.Path;

public class Table {
    public final Path file;
    public final TableHeader header;

    public Table(TableHeader header, Path file) {
        this.header = header;
        this.file = file;
    }
}
