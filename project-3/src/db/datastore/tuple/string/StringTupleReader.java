package db.datastore.tuple.string;

import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class StringTupleReader implements TupleReader {
    private final TableInfo table;
    private Scanner tableFile;

    public StringTupleReader(TableInfo tableInfo, Scanner scanner) {
        this.table = tableInfo;
        this.tableFile = scanner;
    }

    public static StringTupleReader get(TableInfo table) {
        try {
            Scanner scanner = new Scanner(new FileInputStream(table.file.toFile()));
            scanner.useDelimiter(",|\\s+");

            return new StringTupleReader(table, scanner);
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load table file; file not found: " + table.file);
            return null;
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
}
