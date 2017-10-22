package db.datastore.tuple.string;

import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class StringTupleWriter implements TupleWriter {
    private final PrintStream output;

    public StringTupleWriter(PrintStream output) {
        this.output = output;
    }

    public StringTupleWriter(OutputStream output) {
        this.output = new PrintStream(output);
    }

    public static StringTupleWriter get(Path path) {
        try {
            return new StringTupleWriter(new PrintStream(Files.newOutputStream(path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Tuple tuple) {
        String line = tuple.fields.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));

        this.output.println(line);
    }

    @Override
    public void flush() {
        this.output.flush();
    }

    @Override
    public void close() {
        this.output.close();
    }
}
