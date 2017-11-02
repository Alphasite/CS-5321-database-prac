package db.datastore.tuple.string;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.performance.DiskIOStatistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * A tuple reader which reads tuples in string form, as specified by the requirements doc.
 *
 * @inheritDoc
 */
public class StringTupleReader implements TupleReader {
    private final Path path;
    private final TableHeader header;
    private Scanner tableFile;

    private Tuple next;

    /**
     * Create a new reader.
     *
     * @param header The header of the output tuples.
     * @param path   The file path for the reader.
     */
    public StringTupleReader(TableHeader header, Path path) {
        this.header = header;
        this.path = path;
        this.tableFile = getScanner(path);
        this.next = null;

        DiskIOStatistics.handles_opened += 1;
    }

    /**
     * Get a new instance of a tuple reader for this file.
     * @param header The header of the new reader.
     * @param path The path of the string form table.
     * @return The new reader.
     */
    public static TupleReader get(TableHeader header, Path path) {
        return new StringTupleReader(header, path);
    }

    /**
     *
     * @param path
     * @return
     */
    private static Scanner getScanner(Path path) {
        try {
            Scanner scanner = new Scanner(Files.newInputStream(path));
            scanner.useDelimiter(",|\\s+");
            return scanner;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple peek() {
        if (this.next == null) {
            this.next = this.getNextTuple();
        }

        return this.next;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean hasNext() {
        return this.peek() != null;
    }

    /**
     * @inheritDoc
     */
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

    /**
     * @return the next tuple from the underlying file, or null if none exists.
     */
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

    /**
     * @inheritDoc
     */
    @Override
    public void seek(int index) {
        this.tableFile.close();
        this.tableFile = getScanner(path);

        for (int i = 0; i < index; i++) {
            this.next();
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.tableFile.close();
        this.tableFile = null;

        DiskIOStatistics.handles_closed += 1;
    }
}
