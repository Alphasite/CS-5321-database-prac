package db.datastore.tuple.string;

import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;
import db.performance.DiskIOStatistics;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

/**
 * @inheritDoc
 */
public class StringTupleWriter implements TupleWriter {
    private final PrintStream output;

    /**
     * Create a new string writer.
     * @param output the stream which tuples will be written to.
     */
    public StringTupleWriter(OutputStream output) {
        this.output = new PrintStream(output);
        DiskIOStatistics.handles_opened += 1;
    }

    /**
     * Get a new writer for this file.
     * @param path The path which the file will be written to.
     * @return The new writer.
     */
    public static TupleWriter get(Path path) {
        try {
            return new StringTupleWriter(new PrintStream(Files.newOutputStream(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void write(Tuple tuple) {
        String line = tuple.fields.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));

        this.output.println(line);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void flush() {
        this.output.flush();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.output.close();
        DiskIOStatistics.handles_closed += 1;
    }
}
