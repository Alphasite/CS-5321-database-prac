package operators.physcial;

import datastore.Table;
import datastore.Tuple;
import operators.AbstractOperator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Scanner;

public class Scan extends AbstractOperator {
    private final Table table;
    private final Path inputFile;
    private Scanner tableFile;

    private Scan(Table table, Path inputFile) {
        this.table = table;
        this.inputFile = inputFile;
        this.reset();
    }

    public static Optional<Scan> setupScan(Table table) {
        if (Files.exists(table.file)) {
            return Optional.of(new Scan(table, table.file));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Tuple> getNextTuple() {

        if (this.tableFile.hasNextLine()) {
            int cellNumber = this.table.header.columnHeaders.size();
            ArrayList<Integer> row = new ArrayList<>();

            for (int i = 0; i < cellNumber; i++) {
                if (this.tableFile.hasNextInt()) {
                    row.add(this.tableFile.nextInt());
                }
            }

            return Optional.of(new Tuple(this.table.header, row));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean reset() {
        try {
            this.tableFile = new Scanner(new FileInputStream(this.inputFile.toFile()));
            this.tableFile.useDelimiter(",|\\s+");
            return true;
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load table file; file not found: " + table.file);
            return false;
        }
    }
}
