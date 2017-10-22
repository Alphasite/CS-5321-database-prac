package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * This operator sorts input tuples in ascending order, based on the columns referenced by a specified header.
 * In case of a tie, order is determined based on the other columns in left-to-right order.
 *
 * @inheritDoc
 */
public class SortOperator implements Operator, UnaryNode<Operator> {
    private final Operator source;

    private final TableHeader sortHeader;
    private Comparator<Tuple> tupleComparator;

    private List<Tuple> buffer;
    private Iterator<Tuple> bufferIterator;

    /**
     * This creates the sort operator.
     * <p>
     * Instantiating this operator will automatically fully evaluate the child db.operators and sort their output,
     * storing the sorted tuples in an internal buffer, so be careful to not sort too large a dataset with the
     * current implementation.
     *
     * @param source      The operator which creates the tuples which shall be sorted
     * @param sortHeaders The header defines the name and sort order of the columns which are to be used for sorting.
     *                    These do no effect the header of the output, use a project for that.
     */
    public SortOperator(Operator source, TableHeader sortHeaders) {
        this.source = source;
        this.sortHeader = sortHeaders;

        this.tupleComparator = new TupleComparator(sortHeader, source.getHeader());

        this.buffer();
        this.reset();
    }

    /**
     * Get the next tuple in sorted order.
     * <p>
     * The tuples are read from the internal sorted buffer.
     *
     * @inheritDoc
     */
    @Override
    public Tuple getNextTuple() {
        if (this.bufferIterator.hasNext()) {
            return (this.bufferIterator.next());
        } else {
            return null;
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    /**
     * @inheritDoc This does not reset the underlying stream, only resets the buffer iterator.
     */
    @Override
    public boolean reset() {
        this.bufferIterator = this.buffer.iterator();
        return true;
    }

    /**
     * Read all the tuples from the child operator then sort them using the provided sort headers.
     */
    private void buffer() {
        this.buffer = new ArrayList<>();

        Tuple tuple;
        while ((tuple = this.source.getNextTuple()) != null) {
            this.buffer.add(tuple);
        }

        this.buffer.sort(tupleComparator);

        this.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void close() {
        this.buffer.clear();
        this.source.close();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return source;
    }

    /**
     * @return The table which is indicates the sort order.
     */
    public TableHeader getSortHeader() {
        return sortHeader;
    }
}
