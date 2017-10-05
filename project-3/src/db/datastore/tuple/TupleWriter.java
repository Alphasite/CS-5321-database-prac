package db.datastore.tuple;

public interface TupleWriter {
    void write(Tuple tuple);

    void flush();

    void close();
}
