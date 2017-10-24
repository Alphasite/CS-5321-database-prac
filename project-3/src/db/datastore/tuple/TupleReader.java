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
     * Retrieve or generate the next tuple in the table. This does not increment the head pointer.
     * <p>
     * This method may pull from child db.operators transforming the tuples as necessary.
     * <p>
     * If there is a tuple, that is returned, else a null is returned.
     *
     * @return The next tuple, or a null.
     */
    Tuple peek();

    /**
     * Indicate whether or not there is a next tuple to retrieve.
     *
     * @return A bool indicating whether or not the next tuple is null.
     */
    boolean hasNext();

    /**
     * Move the read header to the tuple with the index.
     *
     * @param index the tuple index.
     */
    void seek(long index);

    void close();
}
