package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.operators.UnaryNode;
import db.operators.physical.Operator;

/**
 *
 */
public interface SortOperator extends Operator, UnaryNode<Operator> {
    /**
     * @return The table which is indicates the sort order.
     */
    TableHeader getSortHeader();

    /**
     * Reset the operator to the specified tuple index such that the next call to <pre>getNextTuple()</pre> will return
     * the tuple with that index. Calling <pre>seek(0)</pre> is equivalent to calling <pre>reset()</pre>.
     * <p>
     * This may reset the internal state of an operator.
     *
     * @return A boolean indicating whether the reset succeeded or failed.
     */
    boolean seek(int index);

    /**
     * Get the index of the tuple that was returned by the last call to <pre>getNextTuple()</pre>.
     * If <pre>getNextTuple()</pre> has not been called since the last time the operator was reset,
     * -1 will be returned.
     *
     * @return The index of the tuple returned by the last call to <pre>getNextTuple()</pre> or -1 if no such tuple exists.
     */
    int getTupleIndex();
}
