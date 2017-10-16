package db.datastore.tuple.string;

import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

import java.io.*;
import java.util.stream.Collectors;

public class StringTupleWriter implements TupleWriter {
    private final PrintStream output;

    public StringTupleWriter(PrintStream output) {
        this.output = output;
    }

    public StringTupleWriter(OutputStream output) {
        this.output = new PrintStream(output);
    }

    public static StringTupleWriter get(File file) {
        try {
            return new StringTupleWriter(
                    new PrintStream(new FileOutputStream(file))
            );
        } catch (FileNotFoundException e) {
            System.err.println("Failed to find file: " + file);
            return null;
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
