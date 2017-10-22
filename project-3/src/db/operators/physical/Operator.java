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
