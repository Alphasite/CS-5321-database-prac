package db.operators.physical.utility;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.ArrayList;
import java.util.List;

public class BlockCacheOperator extends AbstractOperator implements UnaryNode<Operator> {
    private boolean valid;
    private Operator operator;
    private List<Tuple> block;
    private int index;
    private int blockSizeBytes;

    public BlockCacheOperator(Operator operator, int blockSizeBytes) {
        this.valid = false;
        this.operator = operator;
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
        return this.operator.getHeader();
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        this.valid = false;
        return this.operator.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTupleIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * @inheritDoc
     */
    @Override
    public void close() {
        this.operator.close();
    }

    private int getPageCapacity() {
        return (this.blockSizeBytes - 2) / 4 / this.operator.getHeader().size();
    }

    public void resetPage() {
        this.index = 0;
    }

    public boolean hasNext() {
        if (!this.valid) {
            this.loadNextBlock();
        }

        if (this.index < this.block.size()) {
            return this.block.get(this.index) != null;
        } else {
            return false;
        }
    }

    public boolean loadNextBlock() {
        this.resetPage();
        this.block.clear();

        for (int i = 0; i < this.getPageCapacity(); i++) {
            Tuple next = operator.getNextTuple();
            this.block.add(next);

            if (next == null)
                break;
        }

        this.valid = true;

        return this.block.get(0) != null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getChild() {
        return operator;
    }
}
