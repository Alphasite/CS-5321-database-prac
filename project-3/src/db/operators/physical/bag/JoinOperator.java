package db.operators.physical.bag;

import db.operators.BinaryNode;
import db.operators.physical.Operator;
import net.sf.jsqlparser.expression.Expression;

/**
 * A join operator with a selection predicate.
 *
 * @inheritDoc
 */
public interface JoinOperator extends Operator, BinaryNode<Operator> {
    /**
     * @return The expression which decides whether or not the tuple is joined, if one is used for this join.
     */
    Expression getPredicate();
}
