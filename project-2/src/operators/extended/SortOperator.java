package operators.extended;

import datastore.TableHeader;
import datastore.Tuple;
import operators.Operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @inheritDoc
 */
public class SortOperator implements Operator {
    private Operator source;
    private List<Tuple> buffer;
    private Iterator<Tuple> bufferIterator;
    private List<Integer> tupleSortPriorityIndex;

    /** This creates the sort operator.
     *
     * Instantiating this operator will automatically fully evaluate the child operators and sort their output,
     * storing the sorted tuples in an internal buffer, so be careful to not sort too large a dataset with the
     * current implementation.
     *
     * @param source The operator which creates the tuples which shall be sorted
     * @param sortHeaders The header defines the name and sort order of the columns which are to be used for sorting.
     *                    These do no effect the header of the output, use a project for that.
     */
    public SortOperator(Operator source, TableHeader sortHeaders) {
        this.source = source;
        this.tupleSortPriorityIndex = new ArrayList<>();

        nextNewColumnLoop:
        for (int i = 0; i < sortHeaders.size(); i++) {
            String alias = sortHeaders.columnAliases.get(i);
            String header = sortHeaders.columnHeaders.get(i);

            boolean notRequireAliasMatch = alias.equals("");

            List<String> sourceAliases = this.source.getHeader().columnAliases;
            List<String> sourceHeaders = this.source.getHeader().columnHeaders;

            for (int j = 0; j < sourceHeaders.size(); j++) {
                boolean aliasMatch = sourceAliases.get(j).equals(alias);
                boolean headerMatch = sourceHeaders.get(j).equals(header);

                if ((notRequireAliasMatch || aliasMatch) && headerMatch) {
                    this.tupleSortPriorityIndex.add(j);
                    continue nextNewColumnLoop;
                }
            }

            throw new RuntimeException("Projection mappings are incorrect. " + alias + "." + header + " has no match.");
        }

        this.buffer();
        this.reset();
    }

    /**
     * Get the next tuple in sorted order.
     *
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
     * This does not reset the underlying stream, only resets the buffer iterator.
     *
     * @inheritDoc
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
        while ((tuple = this.source.getNextTuple())!=null) {
            this.buffer.add(tuple);
        }

        this.buffer.sort((a, b) -> {
            for (Integer tupleIndex : this.tupleSortPriorityIndex) {
                Integer leftField = a.fields.get(tupleIndex);
                Integer rightField = b.fields.get(tupleIndex);

                int result = Integer.compare(leftField, rightField);

                if (result != 0) {
                    return result;
                }
            }

            return 0;
        });

        this.reset();
    }
}
