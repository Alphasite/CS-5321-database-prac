package db.operators.bag;

import db.datastore.TableHeader;
import db.datastore.Tuple;
import net.sf.jsqlparser.expression.Expression;
import db.operators.Operator;
import db.query.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.List;

/**
 * This operator performs a join between the tuples of the two child db.operators.
 *
 * @inheritDoc
 */
public class JoinOperator implements Operator {
    final Operator left;
    final Operator right;

    private Tuple leftTupleCache;
    private TableHeader tableHeader;
    private ExpressionEvaluator evaluator;

    /** Create an object which joins left and right tuples.
     * @param left The operator which generates the left hand tuples.
     * @param right The operator which generates the right hand tuples.
     */
    public JoinOperator(Operator left, Operator right) {
        this.left = left;
        this.right = right;
        this.reset();

        int tableWidth = left.getHeader().size() + right.getHeader().size();

        List<String> headings = new ArrayList<>(tableWidth);
        List<String> aliases = new ArrayList<>(tableWidth);

        headings.addAll(left.getHeader().columnHeaders);
        headings.addAll(right.getHeader().columnHeaders);

        aliases.addAll(left.getHeader().columnAliases);
        aliases.addAll(right.getHeader().columnAliases);

        this.tableHeader = new TableHeader(aliases, headings);
    }

    /** Create an object which joins left and right tuples and filters results based on a conditional clause
     * @param left The operator which generates the left hand tuples.
     * @param right The operator which generates the right hand tuples.
     * @param expression The expression to evaluate the resulting tuples on.
     */
    public JoinOperator(Operator left, Operator right, Expression expression) {
        this(left, right);
        this.evaluator = new ExpressionEvaluator(expression, this.getHeader());
    }

    /**
     * @inheritDoc
     *
     * This method generates tuples by doing a cross product of the left and right operator's tuples.
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
        return this.tableHeader;
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


    /** This method trys to get the next left hand tuple and then resets the right hand operator,
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

    public Expression getPredicate() {
        return evaluator.getExpression();
    }
}
