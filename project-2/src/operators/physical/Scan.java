package operators.physical;

import datastore.TableHeader;
import datastore.TableInfo;
import datastore.Tuple;
import operators.AbstractOperator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class Scan extends AbstractOperator {
    private final TableInfo table;
    private Scanner tableFile;

    public Scan(TableInfo tableInfo) {
        this.table = tableInfo;
        this.reset();
    }

    @Override
    public Tuple getNextTuple() {

        if (this.tableFile.hasNextLine()) {
            int cellNumber = this.table.header.columnHeaders.size();
            ArrayList<Integer> row = new ArrayList<>();

            for (int i = 0; i < cellNumber; i++) {
                if (this.tableFile.hasNextInt()) {
                    row.add(this.tableFile.nextInt());
                }
            }

            return (new Tuple(row));
        } else {
            return null;
        }
    }

    @Override
    public TableHeader getHeader() {
        return this.table.header;
    }

    @Override
    public boolean reset() {
        try {
            this.tableFile = new Scanner(new FileInputStream(this.table.file.toFile()));
            this.tableFile.useDelimiter(",|\\s+");
            return true;
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load table file; file not found: " + table.file);
            return false;
        }
    }
}
