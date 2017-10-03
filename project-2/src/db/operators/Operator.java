package db.operators;

import db.datastore.TableHeader;
import db.datastore.Tuple;

import java.io.PrintStream;

/**
 * This class represents an operator, which is an object which generates a stream of tuples which can then be operated
 * upon. It also provides a schema for the tuple as created by this operator.
 *
 * The operator can be reset back to a zero state, reading from the top of the origin table.
 */
public interface Operator {


    /** Retrieve or generate the next tuple in the table.
     *
     * This method may pull from child db.operators transforming the tuples as necessary.
     *
     * If there is a tuple, that is returned, else a null is returned.
     *
     * @return The next tuple, or a null.
     */
    Tuple getNextTuple();


    /** Get the schema for the tuples produced by this operator.
     * @return The tuples schema.
     */
    TableHeader getHeader();


    /** Reset the operator and any child db.operators, generating tuples from the begining of the table again.
     *
     * This may reset the internal state of an operator.
     *
     * @return A boolean indicating whether the reset succeeded or failed.
     */
    boolean reset();


    /** Write the table header and rows to the provided print stream.
     * @param stream The print stream to which the table is written.
     * @return The number of tuples outputted.
     */
    default int dump(PrintStream stream, boolean extended) {
        if (extended) {
            stream.println(this.getHeader());
        }

        int i = 0;
        Tuple record;
        while ((record = this.getNextTuple()) != null) {
            i += 1;

            if (extended) {
                stream.println(i + ": " + record);
            } else {
                // Not the most efficient but will do
                String output = record.fields.toString();
                stream.println(output.substring(1, output.length() - 1).replace(" ", ""));
            }
        }

        return i;
    }
}
