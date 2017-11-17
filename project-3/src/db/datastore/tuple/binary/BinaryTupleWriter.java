package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;
import db.performance.DiskIOStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Writes tuples in a binary format as specified by the requirements document.
 *
 * @inheritDoc
 */
public class BinaryTupleWriter implements TupleWriter {
    private final TableHeader header;
    private final FileChannel channel;

    private final ByteBuffer bb;

    private int tuples_written;

    /**
     * Create a new writer with the provided header and write it to the specified channel.
     *
     * @param header  The header of the input tuples.
     * @param channel The output channel.
     */
    public BinaryTupleWriter(TableHeader header, FileChannel channel) {
        this.header = header;
        this.channel = channel;

        this.bb = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        this.clearPage();

        this.tuples_written = 0;
    }

    /**
     * Create a new writer outputting binary tuples to the specified file object
     * @param header The relation header, used to properly size the buffers
     * @param file The output file. Will be created if it doesn't exist
     * @return The writer instance.
     */
    public static BinaryTupleWriter get(TableHeader header, Path file) {
        try {
            // Create file if it doesn't exist
            return new BinaryTupleWriter(header, FileChannel.open(
                    file, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void write(Tuple tuple) {
        int offset = this.getTupleOffset();

        this.tuples_written += 1;
        this.bb.asIntBuffer().put(1, this.tuples_written);

        for (int i = 0; i < tuple.fields.size(); i++) {
            this.bb.asIntBuffer().put(offset + i, tuple.fields.get(i));
        }

        if (this.getRemainingCapacity() <= 0) {
            this.flush();
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void flush() {
        try {
            while (this.bb.hasRemaining()) {
                this.channel.write(this.bb);
            }

            this.bb.clear();
            this.clearPage();

            DiskIOStatistics.writes += 1;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Zero the page buffer.
     */
    private void clearPage() {
        for (int i = 0; i < Database.PAGE_SIZE / 4; i++) {
            this.bb.asIntBuffer().put(i, 0);
        }

        this.tuples_written = 0;
        this.bb.asIntBuffer().put(0, this.header.tableIdentifiers.size());
        this.bb.asIntBuffer().put(1, 0);
    }

    /**
     * @return The number of tuples which can be written to this page.
     */
    private int getRemainingCapacity() {
        return (1024 - this.getTupleOffset()) / (this.header.size());
    }

    /**
     * @return The offset of the tuple, relative to the start of the page.
     */
    private int getTupleOffset() {
        return 2 + this.tuples_written * this.header.size();
    }
}
