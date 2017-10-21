package db.datastore.tuple;

public interface TupleReader {
    Tuple next();

    void seek(long index);
}
