package db.datastore.tuple.string;

import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.stream.Collectors;

public class StringTupleWriter implements TupleWriter {
    private final TableInfo tableInfo;
    private final PrintStream output;

    public StringTupleWriter(TableInfo tableInfo, PrintStream output) {
        this.tableInfo = tableInfo;
        this.output = output;
    }

    public static StringTupleWriter getWriter(TableInfo tableInfo) {
        try {
            return new StringTupleWriter(
                    tableInfo,
                    new PrintStream(new FileOutputStream(tableInfo.file.toFile()))
            );
        } catch (FileNotFoundException e) {
            System.err.println("Failed to find file: " + tableInfo.file);
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
}
