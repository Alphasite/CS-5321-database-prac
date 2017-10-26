package db.operators.physical.utility;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.datastore.tuple.TupleWriter;
import db.datastore.tuple.binary.BinaryTupleReader;
import db.datastore.tuple.binary.BinaryTupleWriter;
import db.datastore.tuple.string.StringTupleReader;
import db.datastore.tuple.string.StringTupleWriter;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.SeekableOperator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class ExternalBlockCacheOperator extends AbstractOperator implements SeekableOperator {
    private final TableHeader header;
    private final Path bufferFile;

    private TupleWriter out;
    private TupleReader in;

    private boolean flushed;

    private static final boolean USE_BINARY_PAGES = true;

    public ExternalBlockCacheOperator(TableHeader header, Path tempDirectory, String fileName) {
        this.header = header;
        this.bufferFile = tempDirectory.resolve(fileName);
        this.out = getWriter(header, bufferFile);
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

    /**
     * @inheritDoc
     */
    @Override
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

    public void flush() {
        this.out.flush();
        this.out.close();
        this.out = null;
        this.in = getReader(header, bufferFile);
        this.flushed = true;
    }

    private TupleReader getReader(TableHeader header, Path path) {
        if (USE_BINARY_PAGES)
            return BinaryTupleReader.get(path);
        else
            return StringTupleReader.get(header, path);
    }

    private TupleWriter getWriter(TableHeader header, Path path) {
        if (USE_BINARY_PAGES)
            return BinaryTupleWriter.get(header, path);
        else
            return StringTupleWriter.get(path);
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

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        // Not implemented, is an internal node.
    }

}
