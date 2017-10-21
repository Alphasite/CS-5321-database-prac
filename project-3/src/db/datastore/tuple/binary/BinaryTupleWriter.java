package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class BinaryTupleWriter implements TupleWriter {
    private final TableHeader header;
    private final FileChannel channel;

    private final ByteBuffer bb;

    private int tuples_written;

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
     * @return
     */
    public static BinaryTupleWriter get(TableHeader header, Path file) {
        try {
            // Create file if it doesn't exist
            return new BinaryTupleWriter(header, FileChannel.open(
                    file, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void write(Tuple tuple) {
        int offset = this.getTupleOffset();

        this.tuples_written += 1;
        this.bb.asIntBuffer().put(1, this.tuples_written);

        for (int i = 0; i < tuple.fields.size(); i++) {
//            System.out.println(this.getRemainingCapacity());
            this.bb.asIntBuffer().put(offset + i, tuple.fields.get(i));
        }

        if (this.getRemainingCapacity() <= 0) {
            this.flush();
        }
    }

    @Override
    public void flush() {
        try {
            while (this.bb.hasRemaining()) {
                this.channel.write(this.bb);
//                System.out.println("wrote:" + this.channel.write(this.bb));
            }

            this.bb.clear();

//            if (this.bb.hasRemaining()) {
//                System.err.println("LEFT OVERS????");
//            }

            this.clearPage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void clearPage() {
        for (int i = 0; i < Database.PAGE_SIZE / 4; i++) {
            this.bb.asIntBuffer().put(i, 0);
        }

        this.tuples_written = 0;
        this.bb.asIntBuffer().put(0, this.header.columnAliases.size());
        this.bb.asIntBuffer().put(1, 0);
    }

    private int getRemainingCapacity() {
        return (1024 - this.getTupleOffset()) / (this.header.size());
    }

    private int getTupleOffset() {
        return 2 + this.tuples_written * this.header.size();
    }
}
