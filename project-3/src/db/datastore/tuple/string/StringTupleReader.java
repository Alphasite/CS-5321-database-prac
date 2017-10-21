package db.datastore.tuple.string;

import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class StringTupleReader implements TupleReader {
    private final TableInfo table;
    private Scanner tableFile;

    public StringTupleReader(TableInfo tableInfo) {
        this.table = tableInfo;
        this.tableFile = getScanner(tableInfo.file.toFile());
    }

    public static StringTupleReader get(TableInfo table) {
        return new StringTupleReader(table);
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
            int cellNumber = this.table.header.columnHeaders.size();
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
    public void seek(int index) {
        this.tableFile = getScanner(this.table.file.toFile());

        for (int i = 0; i < index; i++) {
            this.next();
        }
    }
}
