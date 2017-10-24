package db.operators.physical.bag;

import db.datastore.Database;
import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.AbstractOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.operators.physical.utility.BlockCacheOperator;
import db.query.visitors.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

/**
 * This operator performs a join between the tuples of the two child db.operators.
 * <p>
 * It uses the block nested loop join.
 *
 * @inheritDoc
 */
public class BlockNestedJoinOperator extends AbstractOperator implements JoinOperator {
    private final BlockCacheOperator left;
    private final Operator right;

    private TableHeader resultHeader;
    private ExpressionEvaluator evaluator;

    private Tuple rightTuple;

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left       The operator which generates the left hand tuples.
     * @param right      The operator which generates the right hand tuples.
     * @param expression The expression to evaluate the resulting tuples on.
     */
    public BlockNestedJoinOperator(Operator left, Operator right, Expression expression) {
        this(left, right, expression, 1);
    }

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left          The operator which generates the left hand tuples.
     * @param right         The operator which generates the right hand tuples.
     * @param expression    The expression to evaluate the resulting tuples on.
     * @param pagesBuffered The number of pages of tuples which live in the cache.
     */
    public BlockNestedJoinOperator(Operator left, Operator right, Expression expression, int pagesBuffered) {
        this.left = new BlockCacheOperator(left, pagesBuffered * Database.PAGE_SIZE);
        this.right = right;
        this.resultHeader = LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader());
        this.reset();

        if (expression != null) {
            this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
        } else {
            this.evaluator = null;
        }
    }

    /**
     * @inheritDoc
     *
     * This method specifically uses block nested join.
     */
    @Override
    protected Tuple generateNextTuple() {
        while (true) {
            if (rightTuple == null) {
                return null;
            }

            // Iterate through the left block, trying to join each to that right tuple
            while (this.left.hasNext()) {
                Tuple joinedTuple = this.left.getNextTuple().join(rightTuple);

                if (evaluator == null || evaluator.matches(joinedTuple)) {
                    return joinedTuple;
                }
            }

            // If the right tuples have run out, reset the right stream and load the next left block
            if (!this.right.hasNextTuple()) {
                this.right.reset();

                // if we're out of left and right tuples, then we're done.
                if (!this.left.loadNextBlock()) {
                    return null;
                }
            }

            // Block is empty so load next right tuple.
            this.rightTuple = this.right.getNextTuple();
            this.left.resetPage();
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public TableHeader getHeader() {
        return this.resultHeader;
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean reset() {
        if (left.reset() && right.reset()) {
            this.rightTuple = this.right.getNextTuple();
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
        this.left.close();
        this.right.close();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Expression getPredicate() {
        return evaluator.getExpression();
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getLeft() {
        return left;
    }

    /**
     * @inheritDoc
     */
    @Override
    public Operator getRight() {
        return right;
    }
}
