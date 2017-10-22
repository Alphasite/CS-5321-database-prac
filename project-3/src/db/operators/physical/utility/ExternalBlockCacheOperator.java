package db.operators.physical.utility;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class ExternalBlockCacheOperator extends AbstractOperator {
    private final TableHeader header;
    private final Path bufferFile;

    private BinaryTupleWriter out;
    private BinaryTupleReader in;

    private boolean flushed;

    public ExternalBlockCacheOperator(TableHeader header, Path tempDirectory, String file) {
        this.header = header;
        this.bufferFile = tempDirectory.resolve(file);
        this.out = BinaryTupleWriter.get(this.getHeader(), this.bufferFile);
        this.in = null;
        this.flushed = false;
    }


    public ExternalBlockCacheOperator(TableHeader header, Path tempDirectory) {
        this(header, tempDirectory, UUID.randomUUID().toString());
    }

    public Path getBufferFile() {
        return bufferFile;
    }

    public void delete() {
        try {
            this.close();
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

    /**
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
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
            this.writeTupleToBuffer(tuple);
        }
    }

    public void writeSourceToBuffer(Operator source, int pagesToIngest) {
        if (flushed) {
            throw new RuntimeException("Cant write to flushed buffer.");
        }

        BlockCacheOperator page = new BlockCacheOperator(source, Database.PAGE_SIZE);

        for (int i = 0; i < pagesToIngest; i++) {
            page.loadNextBlock();
            this.writeSourceToBuffer(page);
        }
    }

    public void flush() {
        this.out.flush();
        this.out.close();
        this.out = null;
        this.in = BinaryTupleReader.get(this.bufferFile);
        this.flushed = true;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.header;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        this.in.seek(0);
        return true;
    }

    @Override
    public boolean reset(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTupleIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        // Not implemented, is an internal node.
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        if (out != null) {
            this.out.flush();
            this.out.close();
        }

        if (this.in != null) {
            this.in.close();
        }
    }
}
