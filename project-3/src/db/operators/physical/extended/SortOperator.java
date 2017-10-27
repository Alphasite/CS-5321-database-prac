package db.operators.physical.extended;

import db.datastore.TableHeader;
import db.operators.UnaryNode;
import db.operators.physical.Operator;
import db.operators.physical.SeekableOperator;

/**
 * Operators which sort the child relation and buffer the output.
 */
public interface SortOperator extends Operator, UnaryNode<Operator>, SeekableOperator {
    /**
     * @return The table which is indicates the sort order.
     */
    TableHeader getSortHeader();

    /**
     * Get the index of the tuple that was returned by the last call to <pre>getNextTuple()</pre>.
     * If <pre>getNextTuple()</pre> has not been called since the last time the operator was reset,
     * -1 will be returned.
     *
     * @return The index of the tuple returned by the last call to <pre>getNextTuple()</pre> or -1 if no such tuple exists.
     */
    long getTupleIndex();
}
