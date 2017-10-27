package db.datastore.tuple;

/**
 * Write tuples to disk.
 */
public interface TupleWriter {
    /**
     * Write the tuple to disk.
     *
     * @param tuple The tuple to write.
     */
    void write(Tuple tuple);

    /**
     * Persist any cached writes to disk.
     */
    void flush();

    /**
     * Close the file descriptor, releasing the resource.
     */
    void close();
}
