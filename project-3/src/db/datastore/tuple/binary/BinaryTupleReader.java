package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;
import db.performance.DiskIOStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * @inheritDoc
 */
public class BinaryTupleReader implements TupleReader {
    private final Path path;
    private final FileChannel channel;

    private final ByteBuffer bb;

    private long index;
    private long pageNumber;


    /**
     * Create a new reader from the file at the specified path
     *
     * @param path    The file path, used for logging.
     * @param channel The file input channel.
     */
    public BinaryTupleReader(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;

        this.bb = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        this.index = -1;
        this.pageNumber = -1;
    }

    /**
     * Get a new instance of a binary reader.
     * @param path The path for the binary file.
     * @return The instance of the reader.
     */
    public static BinaryTupleReader get(Path path) {
        try {
            return new BinaryTupleReader(path, FileChannel.open(path, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple peek() {
        if (this.index == -1 || this.getNumberOfTuples() <= this.index) {
            if (loadPage(channel, bb)) {
                this.index = 0;
                this.pageNumber += 1;
            } else {
                return null;
            }
        }

        return this.getTupleOnPage(this.index);
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean hasNext() {
        return this.peek() != null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Tuple next() {
        Tuple next = this.peek();
        this.index += 1;
        return next;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void seek(long index) {
        if (this.index == -1) {
            loadPage(channel, bb);
            this.index = 0;
        }

        try {
            long page = index / this.getCapacity();
            long offset = index % this.getCapacity();


            if (this.pageNumber != page) {
                this.channel.position(page * Database.PAGE_SIZE);
                this.loadPage(channel, bb);
            }

            this.pageNumber = page;
            this.index = offset;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void seek(long pageIndex, long tupleIndex) {
        try {
            if (this.pageNumber != pageIndex) {
                this.channel.position(pageIndex * Database.PAGE_SIZE);
                this.loadPage(channel, bb);
            }

            this.pageNumber = pageIndex;
            this.index = tupleIndex;
        } catch (IOException e) {
            throw new RuntimeException(e);
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
     * Load the next page from the channel.
     * @param channel The file channel to pull the page from/
     * @param bb The byte buffer to populate.
     * @return Whether or not the page loaded.
     */
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
            e.printStackTrace();
            return false;
        }

        DiskIOStatistics.reads += 1;

        return true;
    }

    /**
     * @return The number of tuples which can fit on a page.
     */
    private int getCapacity() {
        return (Database.PAGE_SIZE - 8) / 4 / this.getTupleSize();
    }

    /**
     * @return The number of columns per tuple.
     */
    private int getTupleSize() {
        return this.bb.asIntBuffer().get(0);
    }

    /**
     * @return The number of tuples in the current page.
     */
    private int getNumberOfTuples() {
        return this.bb.asIntBuffer().get(1);
    }

    /**
     * @param index The page offset of the tuple which is to be loaded.
     * @return The tuple at the specified offset.
     */
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
