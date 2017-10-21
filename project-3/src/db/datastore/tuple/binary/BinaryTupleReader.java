package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BinaryTupleReader implements TupleReader {
    private final Path path;
    private final FileChannel channel;

    private final ByteBuffer bb;

    private long index;


    public BinaryTupleReader(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;

        this.bb = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        this.index = -1;
    }

    public static BinaryTupleReader get(Path path) {
        try {
            return new BinaryTupleReader(
                    path,
                    new FileInputStream(path.toFile()).getChannel()
            );
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load table file; file not found: " + path);
            return null;
        }
    }

    @Override
    public Tuple next() {
        if (this.index == -1 || this.getNumberOfTuples() <= this.index) {
            if (loadPage(channel, bb)) {
                this.index = 0;
            } else {
                return null;
            }
        }

        return this.getTupleOnPage(this.index++);
    }

    @Override
    public void seek(long index) {
        if (this.index == -1) {
            loadPage(channel, bb);
            this.index = 0;
        }

        try {
            long page = index / this.getCapacity();
            this.channel.position(page * Database.PAGE_SIZE);
            this.loadPage(channel, bb);
            this.index = index % this.getCapacity();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean loadPage(FileChannel channel, ByteBuffer bb) {
        try {
            long len = channel.read(bb);

            if (len == Database.PAGE_SIZE) {
                bb.flip();
                bb.clear();
            } else if (len == -1) {
                // TODO proper logging.
                // System.out.println("Reached end of binary file:" + this.table.file);
                return false;
            } else {
                System.err.println("Error reading binary file: " + path + " Read only " + len + "bytes");
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error reading binary file:" + path);
            return false;
        }

        return true;
    }

    private int getCapacity() {
        return (Database.PAGE_SIZE - 8) / 4 / this.getTupleSize();
    }

    private int getTupleSize() {
        return this.bb.asIntBuffer().get(0);
    }

    private int getNumberOfTuples() {
        return this.bb.asIntBuffer().get(1);
    }

    private Tuple getTupleOnPage(long index) {
        if (this.getNumberOfTuples() <= index) {
            return null;
        }

        long startOffset = 2 + this.getTupleSize() * index;

        List<Integer> tupleBacking = new ArrayList<>(this.getTupleSize());

        for (long i = startOffset; i < startOffset + this.getTupleSize(); i++) {
            tupleBacking.add(this.bb.asIntBuffer().get((int) i));
        }

        return new Tuple(tupleBacking);
    }
}
