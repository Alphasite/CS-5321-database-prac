package db.operators.physical.utility;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory buffer that reads from source source one block at a time, allowing to retrieve previously accessed
 * tuples without recomputing the whole query
 */
public class BlockCacheOperator extends AbstractOperator implements UnaryNode<Operator> {
    private Operator source;
    private int blockSizeBytes;

    private List<Tuple> block;
    private int index;
    private boolean blockLoaded;

    public BlockCacheOperator(Operator source, int blockSizeBytes) {
        this.blockLoaded = false;
        this.source = source;
        this.index = 0;
        this.blockSizeBytes = blockSizeBytes;
        this.block = new ArrayList<>(this.getPageCapacity());
    }

    /**
     * @inheritDoc
     */
    @Override
    protected Tuple generateNextTuple() {
        if (this.hasNext()) {
            return this.block.get(this.index++);
        } else {
            return null;
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.source.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        this.blockLoaded = false;
        return this.source.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.source.close();
    }

    private int getPageCapacity() {
        return (this.blockSizeBytes - 2) / 4 / this.source.getHeader().size();
    }

    public void resetPage() {
        this.index = 0;
    }

    public boolean hasNext() {
        if (!this.blockLoaded) {
            this.loadNextBlock();
        }

        if (this.index < this.block.size()) {
            return this.block.get(this.index) != null;
        } else {
            return false;
        }
    }

    /**
     * Erase current buffer contents and read a block from source
     *
     * @return
     */
    public boolean loadNextBlock() {
        this.resetPage();
        this.block.clear();

        for (int i = 0; i < this.getPageCapacity(); i++) {
            Tuple next = source.getNextTuple();
            this.block.add(next);

            if (next == null)
                break;
        }

        this.blockLoaded = true;

        return this.block.get(0) != null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return source;
    }
}
