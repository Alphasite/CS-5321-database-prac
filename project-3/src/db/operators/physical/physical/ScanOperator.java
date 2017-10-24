package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.string.StringTupleReader;
import db.operators.physical.AbstractOperator;
import db.operators.physical.PhysicalTreeVisitor;

/**
 * An operator which reads a file and parses it according to the schema in the catalogue, producing tuples.
 *
 * @inheritDoc
 */
public class ScanOperator extends AbstractOperator {
    private final TableInfo table;
    private TupleReader reader;

    public ScanOperator(TableInfo tableInfo) {
        this.table = tableInfo;
        this.reset();
    }

    /**
     * This method reads the next tuple from the file,
     * the file is parsed according to the scheme provided in the catalog.
     *
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        return this.reader.next();
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.table.header;
    }

    /**
     * This method creates a new underlying file stream to the table file.
     * <p>
     * This steam points to the head of the file.
     * <p>
     * If there is an error finding the file it fails, it returns false.
     *
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        if (this.table.binary) {
            this.reader = BinaryTupleReader.get(this.table.file);
        } else {
            this.reader = StringTupleReader.get(this.table.header, this.table.file);
        }

        return true;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.reader.close();
    }

    /**
     * @return the underlying table info instance.
     */
    public TableInfo getTable() {
        return table;
    }
}
