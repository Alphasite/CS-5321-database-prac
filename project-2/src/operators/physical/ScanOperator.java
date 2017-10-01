package operators.physical;

import datastore.TableHeader;
import datastore.TableInfo;
import datastore.Tuple;
import operators.Operator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

/** An operator which reads a file and parses it according to the schema in the catalogue, producing tuples.
 * @inheritDoc
 */
public class ScanOperator implements Operator {
    private final TableInfo table;
    private Scanner tableFile;

    public ScanOperator(TableInfo tableInfo) {
        this.table = tableInfo;
        this.reset();
    }

    /** This method reads the next tuple from the file,
     * the file is parsed according to the scheme provided in the catalog.
     *
     * @inheritDoc
     */
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

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.table.header;
    }

    /** This method creates a new underlying file stream to the table file.
     *
     * This steam points to the head of the file.
     *
     * If there is an error finding the file it fails, it returns false.
     *
     * @inheritDoc
     */
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
