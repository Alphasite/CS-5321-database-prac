package db.query.optimizer;

import db.operators.logical.LogicalOperator;

/**
 * Abstract representation of a left-deep join tree, with heuristics to estimate its cost.
 */
class JoinPlan {

    private int estimatedTupleCount;
    private int joinCost;

    private JoinPlan leftChild;

    /**
     * The underlying logical query plan tree
     */
    private LogicalOperator rightChild;

    public JoinPlan(LogicalOperator baseOp) {

    }
}
