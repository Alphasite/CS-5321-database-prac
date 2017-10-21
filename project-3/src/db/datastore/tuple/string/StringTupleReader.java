package db.datastore.tuple.string;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

public class StringTupleReader implements TupleReader {
    private final Path path;
    private final TableHeader header;
    private Scanner tableFile;

    public StringTupleReader(TableHeader header, Path path) {
        this.header = header;
        this.path = path;
        this.tableFile = getScanner(path.toFile());
    }

    public static StringTupleReader get(TableHeader header, Path path) {
        return new StringTupleReader(header, path);
    }

    private static Scanner getScanner(File file) {
        try {
            Scanner scanner = new Scanner(new FileInputStream(file));
            scanner.useDelimiter(",|\\s+");
            return scanner;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple next() {
        if (this.tableFile.hasNextLine()) {
            int cellNumber = this.header.columnHeaders.size();
            ArrayList<Integer> row = new ArrayList<>();

            for (int i = 0; i < cellNumber; i++) {
                if (this.tableFile.hasNextInt()) {
                    row.add(this.tableFile.nextInt());
                } else {
                    return null;
                }
            }

            return (new Tuple(row));
        } else {
            return null;
        }
    }

    @Override
    public void seek(long index) {
        this.tableFile = getScanner(this.path.toFile());

        for (int i = 0; i < index; i++) {
            this.next();
        }
    }
}
