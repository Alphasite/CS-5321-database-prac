package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleWriter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class BinaryTupleWriter implements TupleWriter {
    private final TableHeader header;
    private final FileChannel channel;

    private final ByteBuffer bb;
    private final int[] page;

    private int tuples_written;

    public BinaryTupleWriter(TableHeader header, FileChannel channel) {
        this.header = header;
        this.channel = channel;

        this.bb = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        this.page = new int[Database.PAGE_SIZE / 4];
        this.clearPage();

        this.tuples_written = 0;
    }

    public static BinaryTupleWriter get(TableHeader header, Path path) {
        try {
            return new BinaryTupleWriter(
                    header,
                    new FileOutputStream(path.toFile()).getChannel()
            );
        } catch (FileNotFoundException e) {
            System.err.println("Failed to find file: " + path);
            return null;
        }
    }

    @Override
    public void write(Tuple tuple) {
        int offset = this.getTupleOffset();

        this.tuples_written += 1;
        this.page[1] = this.tuples_written;

        for (int i = 0; i < tuple.fields.size(); i++) {
            System.out.println(this.getRemainingCapacity());
            this.page[offset + i] = tuple.fields.get(i);
        }

        if (this.getRemainingCapacity() <= 0) {
            this.flush();
        }
    }

    @Override
    public void flush() {
        try {
            this.bb.asIntBuffer().put(this.page);

            while (this.bb.hasRemaining()) {
                System.out.println("wrote:" + this.channel.write(this.bb));
            }

            this.bb.clear();

            if (this.bb.hasRemaining()) {
                System.err.println("LEFT OVERS????");
            }

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
        for (int i = 0; i < this.page.length; i++) {
            this.page[i] = 0;
        }

        this.tuples_written = 0;
        this.page[0] = this.header.columnAliases.size();
        this.page[1] = 0;
    }

    private int getRemainingCapacity() {
        return (1024 - this.getTupleOffset()) / (this.header.size());
    }

    private int getTupleOffset() {
        return 2 + this.tuples_written * this.header.size();
    }
}
