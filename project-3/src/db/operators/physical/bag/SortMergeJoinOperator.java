package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.extended.SortOperator;
import db.query.visitors.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

public class SortMergeJoinOperator implements JoinOperator {

    private SortOperator left, right;
    private TableHeader resultHeader;
    private ExpressionEvaluator evaluator;
    private Tuple leftTuple;
    private int lastMatchingRight;

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left       The operator which generates the left hand tuples. Must be a SortOperator.
     * @param right      The operator which generates the right hand tuples. Must be a SortOperator.
     * @param expression The expression to evaluate the resulting tuples on.
     */
    public SortMergeJoinOperator(SortOperator left, SortOperator right, Expression expression) {
        this.left = left;
        this.right = right;
        this.resultHeader = LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader());

        assert expression != null; // "you may assume that ... the join tree will contain at least one equality condition"
        this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
        this.reset();
    }

    @Override
    public Expression getPredicate() {
        return evaluator.getExpression();
    }

    @Override
    public Operator getLeft() {
        return left;
    }

    @Override
    public Operator getRight() {
        return right;
    }

    @Override
    public Tuple getNextTuple() {
        while (this.leftTuple != null) {
            Tuple rightTuple = this.right.getNextTuple();

            if (rightTuple == null) {

                if (lastMatchingRight == -1) {
                    // this means this.leftTuple > rightTuple for all tuples in this.right
                    break;
                } else {
                    this.leftTuple = this.left.getNextTuple();
                    this.right.reset(this.lastMatchingRight);
                    this.lastMatchingRight = -1;
                }

            } else if (this.leftTuple.compareTo(rightTuple) < 0) {
                this.leftTuple = this.left.getNextTuple();
            } else if (this.leftTuple.compareTo(rightTuple) > 0) {
                continue;
            } else /* this.leftTuple.compareTo(rightTuple) == 0 */ {
                if (this.lastMatchingRight == -1) {
                    this.lastMatchingRight = this.right.getTupleIndex();
                }
                return this.leftTuple.join(rightTuple);
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
            this.leftTuple = this.left.getNextTuple();
            this.lastMatchingRight = -1;
            return true;
        } else {
            return false;
        }
    }

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

    @Override
    public void close() {
        left.close();
        right.close();
    }
}
