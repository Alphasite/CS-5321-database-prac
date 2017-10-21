package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class ExternalBlockCacheOperator implements Operator {
    private final Path bufferFile;
    private final TupleWriter out;
    private final Operator source;

    private TupleReader in;


    public ExternalBlockCacheOperator(Operator source, Path tempDirectory) {
        this.source = source;
        this.bufferFile = tempDirectory.resolve(UUID.randomUUID().toString());
        this.out = BinaryTupleWriter.get(this.getHeader(), this.bufferFile.toFile());
        this.in = null;
    }

    public Path getBufferFile() {
        return bufferFile;
    }

    public void delete() {
        try {
            Files.deleteIfExists(bufferFile);
        } catch (IOException e) {
            System.out.println("Failed to delete:" + bufferFile);
        }
    }

    public void seek(long index) {
        this.initialiseIfNeeded();
        this.in.seek(index);
    }

    @Override
    public Tuple getNextTuple() {
        this.initialiseIfNeeded();
        return this.in.next();
    }

    private void initialiseIfNeeded() {
        if (this.in == null) {
            int i = 0;
            Tuple tuple;
            while ((tuple = this.source.getNextTuple()) != null) {
                this.out.write(tuple);
                System.out.println("wrote:" + ++i);
            }

            this.out.flush();

            this.in = BinaryTupleReader.get(this.bufferFile);
        }
    }

    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    @Override
    public boolean reset() {
        this.initialiseIfNeeded();
        this.in.seek(0);
        return true;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        // Not implemented, is an internal node.
    }
}
