package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.extended.SortOperator;
import db.operators.physical.extended.TupleComparator;
import db.query.visitors.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

/**
 * A join which uses the sort merge join to join the child relations.
 * <p>
 * It expects its children to be sorted.
 *
 * @inheritDoc
 */
public class SortMergeJoinOperator extends AbstractOperator implements JoinOperator {

    private SortOperator left, right;
    private TupleComparator tupleComparator;
    private TableHeader resultHeader;
    private long lastMatchingRight;
    private ExpressionEvaluator evaluator;

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left       The operator which generates the left hand tuples. Must be a SortOperator.
     * @param right      The operator which generates the right hand tuples. Must be a SortOperator.
     * @param expression An expression that contains all of the non-equijoin conditions between these two operators, or null.
     */
    public SortMergeJoinOperator(SortOperator left, SortOperator right, Expression expression) {
        this.left = left;
        this.right = right;
        this.tupleComparator = new TupleComparator(left.getSortHeader(), left.getHeader(), right.getSortHeader(), right.getHeader());
        this.resultHeader = LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader());

        if (expression != null) {
            this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
        } else {
            this.evaluator = null;
        }

        this.reset();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Expression getPredicate() {
        if (this.evaluator != null) {
            return this.evaluator.getExpression();
        } else {
            return null;
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getLeft() {
        return this.left;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getRight() {
        return this.right;
    }

    /**
     * @inheritDoc
     */
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
                    this.right.seek(this.lastMatchingRight);
                    this.lastMatchingRight = -1;
                }

            } else {
                int compareResult = this.tupleComparator.compare(leftTuple, rightTuple);

                if (compareResult < 0) {
                    this.left.getNextTuple();

                    if (this.lastMatchingRight != -1) {
                        this.right.seek(this.lastMatchingRight);
                        this.lastMatchingRight = -1;
                    }
                } else if (compareResult > 0) {
                    this.right.getNextTuple();
                } else /* compareResult == 0 */ {
                    if (this.lastMatchingRight == -1) {
                        this.lastMatchingRight = this.right.getTupleIndex();
                    }
                    this.right.getNextTuple();

                    Tuple joinedTuple = leftTuple.join(rightTuple);
                    if (evaluator == null || evaluator.matches(joinedTuple)) {
                        return joinedTuple;
                    }
                }
            }
        }

        // no more tuples
        return null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return resultHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        if (this.left.reset() && this.right.reset()) {
            this.lastMatchingRight = -1;
            return true;
        } else {
            return false;
        }
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
        left.close();
        right.close();
    }
}
