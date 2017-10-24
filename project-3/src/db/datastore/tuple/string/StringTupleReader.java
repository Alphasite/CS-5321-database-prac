package db.datastore.tuple.string;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

public class StringTupleReader implements TupleReader {
    private final Path path;
    private final TableHeader header;
    private Scanner tableFile;

    private Tuple next;

    public StringTupleReader(TableHeader header, Path path) {
        this.header = header;
        this.path = path;
        this.tableFile = getScanner(path);
        this.next = null;
    }

    public static StringTupleReader get(TableHeader header, Path path) {
        return new StringTupleReader(header, path);
    }

    private static Scanner getScanner(Path path) {
        try {
            Scanner scanner = new Scanner(Files.newInputStream(path));
            scanner.useDelimiter(",|\\s+");
            return scanner;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple peek() {
        if (this.next == null) {
            this.next = this.getNextTuple();
        }

        return this.next;
    }

    @Override
    public boolean hasNext() {
        return this.peek() != null;
    }

    @Override
    public Tuple next() {
        if (this.next == null) {
            return getNextTuple();
        } else {
            Tuple next = this.next;
            this.next = null;
            return next;
        }
    }

    private Tuple getNextTuple() {
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
        this.tableFile.close();
        this.tableFile = getScanner(path);

        for (int i = 0; i < index; i++) {
            this.next();
        }
    }

    @Override
    public void close() {
        this.tableFile.close();
        this.tableFile = null;
    }
}
