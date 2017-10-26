package db.datastore.tuple;

public interface TupleWriter {
    // TODO: needs some documentation
    void write(Tuple tuple);
    void flush();
    void close();
}
