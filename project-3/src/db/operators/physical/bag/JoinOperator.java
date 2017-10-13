package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.BinaryNode;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.Operator;
import db.operators.physical.PhysicalTreeVisitor;
import db.query.visitors.ExpressionEvaluator;
import net.sf.jsqlparser.expression.Expression;

/**
 * This operator performs a join between the tuples of the two child db.operators.
 * <p>
 * It uses the tuple nested loop join.
 *
 * @inheritDoc
 */
public class JoinOperator implements Operator, BinaryNode<Operator> {
    private final Operator left;
    private final Operator right;

    private Tuple leftTupleCache;
    private TableHeader resultHeader;
    private ExpressionEvaluator evaluator;

    /**
     * Create an object which joins left and right tuples.
     *
     * @param left  The operator which generates the left hand tuples.
     * @param right The operator which generates the right hand tuples.
     * @param resultHeader The resulting tuple header
     */
    public JoinOperator(Operator left, Operator right, TableHeader resultHeader) {
        this.left = left;
        this.right = right;
        this.resultHeader = resultHeader;
        this.reset();
    }

    public JoinOperator(Operator left, Operator right) {
        this(left, right, LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader()));
    }

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause
     *
     * @param left       The operator which generates the left hand tuples.
     * @param right      The operator which generates the right hand tuples.
     * @param expression The expression to evaluate the resulting tuples on.
     */
    public JoinOperator(Operator left, Operator right, Expression expression) {
        this(left, right);

        if (expression != null) {
            this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
        } else {
            this.evaluator = null;
        }
    }

    /**
     * @inheritDoc This method generates tuples by doing a cross product of the left and right operator's tuples.
     * It increments the left tuple then scans the right operator, repeating until done.
     */
    @Override
    public Tuple getNextTuple() {
        if (this.leftTupleCache == null) {
            // No more tuple on left op : we are done
            return null;
        }

        // Get the rhs tuple and if necessary wrap around the lhs
        Tuple rightTuple, candidate = null;
        boolean foundMatch = false;

        while (!foundMatch) {

            // try to get a right hand tuple
            while ((rightTuple = this.right.getNextTuple()) == null) {
                // If there is none, then increment the left hand operator and then
                // reset the right hand operator and try to get another tuple.
                if (!loadNextLeftTuple()) {
                    return null;
                }
            }

            candidate = leftTupleCache.join(rightTuple);
            if (evaluator == null || evaluator.matches(candidate)) {
                foundMatch = true;
            }
        }

        // Return the joined tuple.
        return candidate;
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
        if (this.left.reset() && this.right.reset()) {
            this.loadNextLeftTuple();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void accept(PhysicalTreeVisitor visitor) {
        visitor.visit(this);
    }


    /**
     * This method tries to get the next left hand tuple and then resets the right hand operator,
     * so that it can try generate more tuples.
     *
     * @return A boolean indicating whether or not tuple generation is done.
     */
    private boolean loadNextLeftTuple() {
        Tuple leftFromChild = this.left.getNextTuple();
        if (leftFromChild != null) {
            leftTupleCache = leftFromChild;
            right.reset();
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return The expression which decides whether or not the tuple is joined, if one is used for this join.
     */
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
