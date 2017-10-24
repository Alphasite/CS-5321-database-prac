package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.extended.TupleComparator;
import net.sf.jsqlparser.expression.Expression;

public class SortMergeJoinOperator extends AbstractOperator implements JoinOperator {

    private SortOperator left, right;
    private TupleComparator tupleComparator;
    private TableHeader resultHeader;
    private int lastMatchingRight;
    private Expression joinExpression;

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left       The operator which generates the left hand tuples. Must be a SortOperator.
     * @param right      The operator which generates the right hand tuples. Must be a SortOperator.
     */
    public SortMergeJoinOperator(SortOperator left, SortOperator right, Expression joinExpression) {
        this.left = left;
        this.right = right;
        this.tupleComparator = new TupleComparator(left.getSortHeader(), left.getHeader(), right.getSortHeader(), right.getHeader());
        this.resultHeader = LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader());
        this.joinExpression = joinExpression;

        this.reset();
    }

    @Override
    public Expression getPredicate() {
        return this.joinExpression;
    }

    @Override
    public Operator getLeft() {
        return this.left;
    }

    @Override
    public Operator getRight() {
        return this.right;
    }

    @Override
    public Tuple generateNextTuple() {
        while (this.left.peekNextTuple() != null) {
            Tuple leftTuple = this.left.peekNextTuple();
            Tuple rightTuple = this.right.peekNextTuple();

            if (rightTuple == null) {

                if (lastMatchingRight == -1) {
                    // this means this.leftTuple > rightTuple for all tuples in this.right
                    break;
                } else {
                    this.left.getNextTuple();
                    this.right.reset(this.lastMatchingRight);
                    this.lastMatchingRight = -1;
                }

            } else if (this.tupleComparator.compare(leftTuple, rightTuple) < 0) {
                this.left.getNextTuple();

                if (this.lastMatchingRight != -1) {
                    this.right.reset(this.lastMatchingRight);
                    this.lastMatchingRight = -1;
                }
            } else if (this.tupleComparator.compare(leftTuple, rightTuple) > 0) {
                this.right.getNextTuple();
            } else /* compare(leftTuple, rightTuple) == 0 */ {
                if (this.lastMatchingRight == -1) {
                    this.lastMatchingRight = this.right.getTupleIndex();
                }
                this.right.getNextTuple();
                return leftTuple.join(rightTuple);
            }
        }

        // no more tuples
        return null;
    }

    @Override
    public TableHeader getHeader() {
        return resultHeader;
    }

    @Override
    public boolean reset() {
        if (this.left.reset() && this.right.reset()) {
            this.lastMatchingRight = -1;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }
}
