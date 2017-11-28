package db;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseStructure {
    public final Path input;
    public final Path output;
    public final Path path;
    public final Path db;
    public final Path expected;
    public final Path tmp;
    public final Path indices;

    public DatabaseStructure(Path path) throws IOException {
        this.path = Files.createTempDirectory("db-tempdir");

        FileUtils.copyDirectory(path.toFile(), this.path.toFile());

        this.input = this.path.resolve("input");
        this.db = this.input.resolve("db");
        this.output = this.path.resolve("output");
        this.expected = this.path.resolve("expected");
        this.tmp = this.path.resolve("tmp");
        this.indices = this.db.resolve("indexes");
    }
}
