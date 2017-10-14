package db.operators.physical.physical;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.UnaryNode;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;

import java.util.ArrayList;
import java.util.List;

public class BlockCacheOperator implements Operator, UnaryNode<Operator> {
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

    @Override
    public Tuple getNextTuple() {
        if (this.hasNext()) {
            return this.block.get(this.index++);
        } else {
            return null;
        }
    }

    @Override
    public TableHeader getHeader() {
        return this.operator.getHeader();
    }

    public boolean reset() {
        this.valid = false;
        return this.operator.reset();
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    private int getPageCapacity() {
        return (this.blockSizeBytes - 2) / 4 / this.operator.getHeader().size();
    }

    public void resetPage() {
        this.index = 0;
    }

    public boolean hasNext() {
        if (!this.valid) {
            this.loadBlock();
        }

        if (this.index < this.block.size()) {
            return this.block.get(this.index) != null;
        } else {
            return false;
        }
    }

    public boolean loadBlock() {
        this.resetPage();
        this.block.clear();

        for (int i = 0; i < this.getPageCapacity(); i++) {
            this.block.add(this.operator.getNextTuple());
        }

        this.valid = true;

        return this.block.get(0) != null;
    }

    @Override
    public Operator getChild() {
        return operator;
    }
}
