package db.operators.physical;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

/**
 * Interface for an operator, which is an object which generates a stream of tuples which can then be operated
 * upon. It also provides a schema for the tuple as created by this operator.
 * <p>
 * The operator can be reset back to a zero state, reading from the top of the origin table.
 */
public interface Operator {


    /**
     * Retrieve or generate the next tuple in the table.
     * <p>
     * This method may pull from child db.operators transforming the tuples as necessary.
     * <p>
     * If there is a tuple, that is returned, else a null is returned.
     *
     * @return The next tuple, or a null.
     */
    Tuple getNextTuple();

    /**
     * Retrieve or generate the next tuple in the table. This does not increment the head pointer.
     * <p>
     * This method may pull from child db.operators transforming the tuples as necessary.
     * <p>
     * If there is a tuple, that is returned, else a null is returned.
     *
     * @return The next tuple, or a null.
     */
    Tuple peekNextTuple();

    /**
     * Indicate whether or not there is a next tuple to retrieve.
     *
     * @return A bool indicating whether or not the next tuple is null.
     */
    default boolean hasNextTuple() {
        return this.peekNextTuple() != null;
    }

    /**
     * Get the schema for the tuples produced by this operator.
     *
     * @return The tuples schema.
     */
    TableHeader getHeader();


    /**
     * Reset the operator and any child db.operators, generating tuples from the beginning of the table again.
     * <p>
     * This may reset the internal state of an operator.
     *
     * @return A boolean indicating whether the reset succeeded or failed.
     */
    boolean reset();

    /**
     * Reset the operator to the specified tuple index. Calling <pre>reset(0)</pre> is equivalent to calling <pre>reset()</pre>.
     * <p>
     * This may reset the internal state of an operator.
     *
     * @return A boolean indicating whether the reset succeeded or failed.
     */
    boolean reset(int index);

    /**
     * Get the index of the tuple that was returned by the last call to <pre>getNextTuple()</pre>.
     * If <pre>getNextTuple()</pre> has not been called since the last time the operator was reset,
     * -1 will be returned.
     *
     * @return The index of the tuple returned by the last call to <pre>getNextTuple()</pre> or -1 if no such tuple exists.
     */
    int getTupleIndex();

    /**
     * Accept a visitor
     *
     * @param visitor the visitor to be accepted.
     */
    void accept(PhysicalTreeVisitor visitor);

    /**
     * Close any readers/writers, releasing file descriptors.
     */
    void close();

    /**
     * Write the table header and rows with the provided Writer
     *
     * @return The number of tuples outputted.
     */
    default int dump(TupleWriter writer) {
        Tuple record;
        int i;
        for (i = 0; (record = this.getNextTuple()) != null; i++) {
            writer.write(record);
        }

        writer.flush();

        return i;
    }
}
