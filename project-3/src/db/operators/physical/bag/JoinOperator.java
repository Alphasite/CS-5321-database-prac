package db.operators.physical.bag;

import db.operators.BinaryNode;
import db.operators.physical.Operator;
import net.sf.jsqlparser.expression.Expression;

public interface JoinOperator extends Operator, BinaryNode<Operator> {
    /**
     * @return The expression which decides whether or not the tuple is joined, if one is used for this join.
     */
    Expression getPredicate();
}
