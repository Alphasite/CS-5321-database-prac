package db.datastore.tuple;

/**
 * Read tuples from disk.
 */
public interface TupleReader {
    /**
     * Return the next tuple.
     *
     * @return The next tuple or null if none exists.
     */
    Tuple next();

    /**
     * Move the read header to the tuple with the index.
     *
     * @param index the tuple index.
     */
    void seek(long index);
}
