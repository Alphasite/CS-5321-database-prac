package db.operators.physical.utility;

import db.datastore.Database;
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
    private final TableHeader header;
    private final Path bufferFile;

    private final TupleWriter out;
    private final TupleReader in;

    private boolean flushed;

    public ExternalBlockCacheOperator(TableHeader header, Path tempDirectory) {
        this.header = header;
        this.bufferFile = tempDirectory.resolve(UUID.randomUUID().toString());
        this.out = BinaryTupleWriter.get(this.getHeader(), this.bufferFile.toFile());
        this.in = BinaryTupleReader.get(this.bufferFile);
        this.flushed = false;
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
        if (!flushed) {
            this.flush();
        }

        this.in.seek(index);
    }

    @Override
    public Tuple getNextTuple() {
        if (!flushed) {
            this.flush();
        }

        return this.in.next();
    }

    public void writeTupleToBuffer(Tuple tuple) {
        if (flushed) {
            throw new RuntimeException("Cant write to flushed buffer.");
        }

        this.out.write(tuple);
    }

    public void writeSourceToBuffer(Operator source) {
        if (flushed) {
            throw new RuntimeException("Cant write to flushed buffer.");
        }

        Tuple tuple;
        while ((tuple = source.getNextTuple()) != null) {
            this.out.write(tuple);
        }
    }

    public void writeSourceToBuffer(Operator source, int pagesToIngest) {
        if (flushed) {
            throw new RuntimeException("Cant write to flushed buffer.");
        }

        BlockCacheOperator page = new BlockCacheOperator(source, Database.PAGE_SIZE);

        for (int i = 0; i < pagesToIngest; i++) {
            page.loadBlock();
            writeSourceToBuffer(page);
        }

        Tuple tuple;
        while ((tuple = source.getNextTuple()) != null) {
            this.writeTupleToBuffer(tuple);
        }
    }

    public void flush() {
        this.out.flush();
        this.out.close();
        this.flushed = true;
    }

    @Override
    public TableHeader getHeader() {
        return this.header;
    }

    @Override
    public boolean reset() {
        this.in.seek(0);
        return true;
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        // Not implemented, is an internal node.
    }
}
