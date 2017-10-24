package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.AbstractOperator;
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
public class InMemorySortOperator extends AbstractOperator implements SortOperator, UnaryNode<Operator> {
    private final Operator source;

    private final TableHeader sortHeader;
    private Comparator<Tuple> tupleComparator;

    private List<Tuple> buffer;
    private Iterator<Tuple> bufferIterator;

    private boolean isSorted;

    private int tupleIndex;

    /**
     * This creates the sort operator with the specified parameters
     * <p>
     * Sorting will be performed when the first tuple is requested
     *
     * @param source      The operator which creates the tuples which shall be sorted
     * @param sortHeaders The header defines the name and sort order of the columns which are to be used for sorting.
     *                    These do no effect the header of the output, use a project for that.
     */
    public InMemorySortOperator(Operator source, TableHeader sortHeaders) {
        this.source = source;
        this.sortHeader = sortHeaders;

        this.tupleComparator = new TupleComparator(sortHeader, source.getHeader());
        this.isSorted = false;

        this.buffer = new ArrayList<>();

        this.tupleIndex = -1;
    }

    /**
     * Get the next tuple in sorted order.
     * <p>
     * The tuples are read from the internal sorted buffer.
     *
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        if (!isSorted) {
            this.buffer();
        }

        if (this.bufferIterator.hasNext()) {
            this.tupleIndex++;
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
        this.tupleIndex = -1;
        this.next = null;
        return true;
    }

    @Override
    public boolean reset(int index) {
        this.bufferIterator = this.buffer.listIterator(index);
        this.tupleIndex = index-1;
        this.next = null;
        return true;
    }

    @Override
    public int getTupleIndex() {
        return this.tupleIndex;
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
        this.isSorted = true;

        this.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
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
     * @inheritDoc
     */
    public TableHeader getSortHeader() {
        return sortHeader;
    }
}
