package db.datastore.tuple.binary;

import db.datastore.TableInfo;
import db.datastore.tuple.Tuple;
import db.datastore.tuple.TupleReader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class BinaryTupleReader implements TupleReader {
    private final TableInfo table;
    private final FileChannel channel;

    private final ByteBuffer bb;
    private final int[] page;

    private int index;


    public BinaryTupleReader(TableInfo table, FileChannel channel) {
        this.table = table;
        this.channel = channel;

        this.page = new int[4096 / 4];
        this.bb = ByteBuffer.allocateDirect(4096);

        this.index = -1;
    }

    public static BinaryTupleReader get(TableInfo table) {
        try {
            return new BinaryTupleReader(
                    table,
                    new FileInputStream(table.file.toFile()).getChannel()
            );
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load table file; file not found: " + table.file);
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

    private boolean loadPage(FileChannel channel, ByteBuffer bb) {
        try {
            long len = channel.read(bb);

            if (len == 4096) {
                bb.flip();
                bb.asIntBuffer().get(page);
                bb.clear();
            } else if (len == -1) {
                // TODO proper logging.
                // System.out.println("Reached end of binary file:" + this.table.file);
                return false;
            } else {
                System.err.println("Error reading binary file:" + this.table.file + " Read only " + len + "bytes");
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error reading binary file:" + this.table.file);
            return false;
        }

        return true;
    }

    private int getTupleSize() {
        return this.page[0];
    }

    private int getNumberOfTuples() {
        return this.page[1];
    }

    private Tuple getTupleOnPage(int index) {
        if (this.getNumberOfTuples() <= index) {
            return null;
        }

        int startOffset = 2 + this.getTupleSize() * index;

        ArrayList<Integer> tupleBacking = new ArrayList<>(this.getTupleSize());

        for (int i = startOffset; i < startOffset + this.getTupleSize(); i++) {
            tupleBacking.add(this.page[i]);
        }

        return new Tuple(tupleBacking);
    }
}