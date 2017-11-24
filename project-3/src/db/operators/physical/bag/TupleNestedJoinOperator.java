package db.operators.physical.bag;

import db.datastore.TableHeader;
import db.datastore.tuple.Tuple;
import db.operators.logical.LogicalJoinOperator;
import db.operators.physical.AbstractOperator;
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
public class TupleNestedJoinOperator extends AbstractOperator implements JoinOperator {
    private final Operator left;
    private final Operator right;

    private Tuple leftTupleCache;
    private TableHeader resultHeader;
    private ExpressionEvaluator evaluator;

    /**
     * Create an object which joins left and right tuples.
     *
     * @param left       The operator which generates the left hand tuples.
     * @param right      The operator which generates the right hand tuples.
     */
    public TupleNestedJoinOperator(Operator left, Operator right) {
        this.left = left;
        this.right = right;
        this.evaluator = null;
        this.resultHeader = LogicalJoinOperator.computeHeader(left.getHeader(), right.getHeader());
        this.reset();
    }

    /**
     * Create an object which joins left and right tuples and filters results based on a conditional clause.
     *
     * @param left  The operator which generates the left hand tuples.
     * @param right The operator which generates the right hand tuples.
     * @param expression The expression to evaluate the resulting tuples on.
     */
    public TupleNestedJoinOperator(Operator left, Operator right, Expression expression) {
        this(left, right);

        if (expression != null)
            this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
    }

    /**
     * @inheritDoc This method generates tuples by doing a cross product of the left and right operator's tuples.
     * It increments the left tuple then scans the right operator, repeating until done.
     */
    @Override
    protected Tuple generateNextTuple() {
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
                if (this.left.hasNextTuple()) {
                    this.leftTupleCache = this.left.getNextTuple();
                    this.right.reset();
                } else {
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
            this.leftTupleCache = this.left.getNextTuple();
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
        return evaluator != null ? evaluator.getExpression() : null;
    }

    /**
     * @inheritDoc
     */
    @Override
    public String getJoinType() {
        return "TNLJ";
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
