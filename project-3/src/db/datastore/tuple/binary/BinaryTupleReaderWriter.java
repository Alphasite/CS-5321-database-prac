package db.datastore.tuple.binary;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReaderWriter;
import db.performance.DiskIOStatistics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * @inheritDoc
 */
public class BinaryTupleReaderWriter implements TupleReaderWriter {
    private final TableHeader header;
    private final Path path;
    private final FileChannel channel;

    private final ByteBuffer bb;

    private int index;
    private int pageNumber;


    /**
     * Create a new reader from the file at the specified path
     *
     * @param path    The file path, used for logging.
     * @param channel The file input channel.
     */
    public BinaryTupleReaderWriter(TableHeader header, Path path, FileChannel channel) {
        this.header = header;
        this.path = path;
        this.channel = channel;

        this.bb = ByteBuffer.allocateDirect(Database.PAGE_SIZE);

        this.clearPage();

        this.index = -1;
        this.pageNumber = -1;

        DiskIOStatistics.handles_opened += 1;
    }

    /**
     * Get a new instance of a binary reader/writer.
     * @param header the header for the tuples.
     * @param path The path for the binary file.
     * @return The instance of the reader.
     */
    public static BinaryTupleReaderWriter get(TableHeader header, Path path) {
        try {
            if (!Files.exists(path)) {
                Files.createFile(path);
            }

            return new BinaryTupleReaderWriter(header, path, FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE));
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
    public void seek(int index) {
        if (this.index == -1) {
            loadPage(channel, bb);
            this.index = 0;
        }

        try {
            int page = index / this.getCapacity();
            int offset = index % this.getCapacity();


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

    /**
     * @inheritDoc
     */
    @Override
    public void write(Tuple tuple) {
        if (this.index == -1 || this.getRemainingCapacity() <= 0) {
            this.loadPage(channel, bb);
            this.index = 0;
            this.pageNumber += 1;
        }

        int offset = this.getTupleOffset();

        this.index += 1;
        this.bb.asIntBuffer().put(1, this.index);

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

            this.loadPage(channel, bb);
            this.index = 0;
            this.pageNumber += 1;

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
            DiskIOStatistics.handles_closed += 1;
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
            int len = channel.read(bb);

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
    private Tuple getTupleOnPage(int index) {
        if (this.getNumberOfTuples() <= index) {
            return null;
        }

        int startOffset = 2 + this.getTupleSize() * index;

        List<Integer> tupleBacking = new ArrayList<>(this.getTupleSize());

        for (int i = startOffset; i < startOffset + this.getTupleSize(); i++) {
            tupleBacking.add(this.bb.asIntBuffer().get(i));
        }

        return new Tuple(tupleBacking);
    }

    /**
     * Zero the page buffer.
     */
    private void clearPage() {
        for (int i = 0; i < Database.PAGE_SIZE / 4; i++) {
            this.bb.asIntBuffer().put(i, 0);
        }

        this.index = 0;
        this.bb.asIntBuffer().put(0, this.header.columnAliases.size());
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
        return 2 + this.index * this.header.size();
    }
}
